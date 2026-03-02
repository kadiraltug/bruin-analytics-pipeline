import os, json, time, random
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "game_events")
RATE = float(os.getenv("RATE_PER_SEC", "20"))
SIM_SPEED_SECONDS = int(os.getenv("SIM_SPEED_SECONDS", str(60 * 5)))
SIM_START_ISO = os.getenv("SIM_START_ISO", "2026-01-01T00:00:00+00:00")

INITIAL_USERS = int(os.getenv("INITIAL_USERS", "2000"))
NEW_USERS_EVERY_SEC = 5
NEW_USERS_BATCH = 10
DAILY_CHURN_PROB = 0.002

SESSION_START_PROB = 0.1
SESSION_END_PROB = 0.04
PURCHASE_PROB = 0.008
AD_IMPRESSION_PROB = 0.15

PLATFORMS = ["ios", "android"]
COUNTRIES = ["DE", "TR", "US", "GB", "FR", "BR", "IN"]
CURRENCIES = ["USD", "EUR"]
IAP_PRICE_POINTS = [0.99, 4.99, 9.99, 19.99, 49.99, 99.99]

APP_VERSIONS = {
    "ios":     [("1.2.1", 0.55), ("1.2.0", 0.25), ("1.1.0", 0.15), ("1.0.0", 0.05)],
    "android": [("1.2.1", 0.45), ("1.2.0", 0.30), ("1.1.0", 0.18), ("1.0.0", 0.07)],
}
DEVICE_MODELS = {
    "ios":     ["iPhone15", "iPhone14", "iPhone13", "iPhone12", "iPad Pro"],
    "android": ["S24", "S23", "Pixel 8", "Pixel 7", "OnePlus 12", "Xiaomi 14"],
}

p = Producer({"bootstrap.servers": BOOTSTRAP})

def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA] delivery failed: {err}", flush=True)

def parse_start_dt(iso: str) -> datetime:
    iso = iso.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso)
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

def weighted_choice(choices):
    """choices: list of (value, weight) tuples"""
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]

class SimClock:
    def __init__(self, start_dt: datetime, speed: int):
        self.current = start_dt
        self.speed = speed
        self._last_real = time.time()

    def tick(self) -> datetime:
        now_real = time.time()
        delta_real = now_real - self._last_real
        self._last_real = now_real
        self.current += timedelta(seconds=self.speed * delta_real)
        return self.current

clock = SimClock(parse_start_dt(SIM_START_ISO), SIM_SPEED_SECONDS)

@dataclass
class User:
    user_id: str
    platform: str
    country: str
    app_version: str
    device_model: str
    skill: float
    is_whale: bool
    is_active: bool = True
    in_session: bool = False
    session_id: str | None = None
    session_start_ts: int | None = None
    level: int = 1
    active_level: int | None = None
    active_level_start_ts: int | None = None

def new_user() -> User:
    platform = random.choice(PLATFORMS)
    return User(
        user_id=fake.uuid4(),
        platform=platform,
        country=random.choice(COUNTRIES),
        app_version=weighted_choice(APP_VERSIONS[platform]),
        device_model=random.choice(DEVICE_MODELS[platform]),
        skill=max(0.05, min(0.95, random.gauss(0.5, 0.15))),
        is_whale=random.random() < 0.03,
    )

def emit_event(u: User, event_name: str, **kwargs) -> dict:
    ts_ms = int(clock.current.timestamp() * 1000)
    return {
        "event_id":    fake.uuid4(),
        "user_id":     u.user_id,
        "session_id":  u.session_id,
        "event_name":  event_name,
        "event_ts":    ts_ms,
        "platform":    u.platform,
        "country":     u.country,
        "app_version": u.app_version,
        "device_model":u.device_model,
        "level":       kwargs.get("level", u.level),
        "result":      kwargs.get("result"),
        "duration_sec":kwargs.get("duration_sec"),
        "revenue_usd": kwargs.get("revenue_usd"),
        "currency":    kwargs.get("currency"),
        "updated_at":  ts_ms,
    }

def produce(event: dict):
    p.produce(
        TOPIC,
        key=event["user_id"],
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    p.poll(0)

users: list[User] = [new_user() for _ in range(INITIAL_USERS)]
active_user_set: set[int] = set(range(len(users)))
_last_new_users_at = time.time()

for u in users:
    produce(emit_event(u, "user_register"))

_ticks_per_sim_day = RATE * 86400 / SIM_SPEED_SECONDS
CHURN_PROB_PER_TICK = DAILY_CHURN_PROB / max(_ticks_per_sim_day, 1)

def _close_session(u: User, ts_ms: int) -> dict:
    duration = max(1, int((ts_ms - (u.session_start_ts or ts_ms)) / 1000))
    event = emit_event(u, "session_end", duration_sec=duration)
    u.in_session = False
    u.session_id = None
    u.session_start_ts = None
    u.active_level = None
    u.active_level_start_ts = None
    return event


def handle_user_action(u: User, idx: int) -> list[dict]:
    ts_ms = int(clock.current.timestamp() * 1000)

    if random.random() < CHURN_PROB_PER_TICK:
        events = []
        if u.in_session:
            events.append(_close_session(u, ts_ms))
        u.is_active = False
        events.append(emit_event(u, "user_churn"))
        active_user_set.discard(idx)
        return events

    if not u.in_session:
        if random.random() < SESSION_START_PROB:
            u.in_session = True
            u.session_id = fake.uuid4()
            u.session_start_ts = ts_ms
            return [emit_event(u, "session_start")]
        return []

    events = []

    if random.random() < SESSION_END_PROB:
        events.append(_close_session(u, ts_ms))
        return events

    if u.active_level is None:
        if random.random() < 0.3:
            u.active_level = u.level
            u.active_level_start_ts = ts_ms
            events.append(emit_event(u, "level_start", level=u.active_level))
    else:
        if random.random() < 0.2:
            difficulty = min(0.4, u.active_level * 0.005)
            win_prob = max(0.1, min(0.9, u.skill - difficulty + 0.5))
            result = "win" if random.random() < win_prob else "fail"
            level_duration = max(10, int((ts_ms - (u.active_level_start_ts or ts_ms)) / 1000))
            events.append(emit_event(
                u, "level_complete",
                level=u.active_level,
                result=result,
                duration_sec=level_duration,
            ))
            if result == "win":
                u.level += 1
            u.active_level = None
            u.active_level_start_ts = None

    if u.active_level is None and u.in_session:
        iap_prob = PURCHASE_PROB * (12 if u.is_whale else 1)
        r = random.random()

        if r < iap_prob:
            if u.is_whale:
                price = weighted_choice([
                    (0.99, 2), (4.99, 5), (9.99, 15),
                    (19.99, 30), (49.99, 30), (99.99, 18),
                ])
            else:
                price = weighted_choice([
                    (0.99, 50), (4.99, 35), (9.99, 15),
                ])
            currency = "EUR" if u.country in ("DE", "FR") else "USD"
            events.append(emit_event(u, "iap_purchase", revenue_usd=price, currency=currency))

        elif r < iap_prob + AD_IMPRESSION_PROB:
            tier1 = u.country in ("US", "GB", "DE", "FR")
            rev = round(
                random.uniform(0.02, 0.06) if tier1 else random.uniform(0.005, 0.02),
                4,
            )
            events.append(emit_event(u, "ad_impression", revenue_usd=rev, currency="USD"))

    return events

_last_compaction_at = time.time()
COMPACTION_EVERY_SECS = 120


def compact_users():
    global users, active_user_set
    active_list = [u for u in users if u.is_active]
    users = active_list
    active_user_set = set(range(len(users)))


while True:
    clock.tick()

    if time.time() - _last_compaction_at >= COMPACTION_EVERY_SECS:
        compact_users()
        _last_compaction_at = time.time()

    if time.time() - _last_new_users_at >= NEW_USERS_EVERY_SEC:
        for _ in range(NEW_USERS_BATCH):
            u_new = new_user()
            users.append(u_new)
            new_idx = len(users) - 1
            active_user_set.add(new_idx)
            produce(emit_event(u_new, "user_register"))
        _last_new_users_at = time.time()

    if not active_user_set:
        time.sleep(1.0 / RATE)
        continue

    u_idx = random.choice(list(active_user_set))
    u = users[u_idx]

    if u.is_active:
        for ev in handle_user_action(u, u_idx):
            produce(ev)

    time.sleep(1.0 / RATE)