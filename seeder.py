#!/usr/bin/env python3
"""
==============================================================================
ENTERPRISE DATA SEEDER & HISTORICAL GENERATOR
==============================================================================
Tự động sinh dữ liệu toàn diện cho hệ sinh thái Big Data & Data Lakehouse:
1. PostgreSQL Source (OLTP CDC): Users, Devices, Cam Bills (với mốc thời gian lịch sử & chu kỳ cước)
2. IoT Camera Logs (JSON): Sự kiện camera, streaming, lỗi mạng lưu trữ theo ngày/tháng
3. Payment Reconciliation (CSV): File đối soát thanh toán định kỳ theo ngày khớp với hóa đơn
==============================================================================
"""

import os
import sys
import time
import json
import gzip
import uuid
import random
import argparse
import subprocess
from datetime import datetime, date, timedelta

# ==============================================================================
# HẰNG SỐ & DANH MỤC DỮ LIỆU
# ==============================================================================
HO = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô", "Dương"]
TEN_DEM = ["Văn", "Thị", "Hồng", "Minh", "Đức", "Anh", "Ngọc", "Thanh", "Quốc", "Gia", "Bảo", "Hải", "Tuấn", "Hữu"]
TEN = ["Hùng", "Lan", "Nam", "Mai", "Cường", "Trang", "Tuấn", "Linh", "Dũng", "Hoa", "Bình", "Thảo", "Long", "Hương", "Huy", "Vy", "Đạt", "Yến", "Tâm", "Phúc"]

DEVICE_TYPES = ["Outdoor_Cam_v2", "Indoor_Cam_360", "Doorbell_Cam", "AI_Dome_Cam_4K", "Solar_Security_Cam"]
DEVICE_PREFIXES = ["Camera Phòng Khách", "Camera Sân Trước", "Camera Ban Công", "Camera Cửa Chính", "Camera Kho Hàng", "Camera Gara", "Camera Sân Thượng"]
PLAN_TYPES = ["monthly", "yearly", "quarterly"]
PLAN_PRICES = {
    "monthly": [100000.0, 150000.0, 200000.0],
    "quarterly": [280000.0, 420000.0, 550000.0],
    "yearly": [1000000.0, 1500000.0, 1800000.0]
}

PAYMENT_GATEWAYS = ["ViettelMoney", "VNPay", "Momo", "MBBank", "Vietcombank", "Techcombank"]
BANK_CODES = ["MBBank", "Vietcombank", "Techcombank", "BIDV", "VietinBank", "VPBank"]

LOG_EVENT_TYPES = [
    "motion_detected", "human_detected", "stream_started", "stream_stopped",
    "connection_lost", "heartbeat", "sd_card_error", "firmware_update"
]
FIRMWARE_VERSIONS = ["v1.4.2", "v2.0.1", "v2.1.0", "v2.2.0-beta"]

# ==============================================================================
# HÀM BỔ TRỢ XỬ LÝ CHUỖI & SQL
# ==============================================================================
def remove_accents(input_str):
    s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ'
    s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy'
    s = ''
    for c in input_str:
        if c in s1:
            s += s0[s1.index(c)]
        else:
            s += c
    return s.lower().replace(" ", "")

def execute_psql(sql_query):
    """
    Thực thi câu lệnh SQL vào postgres-source thông qua docker exec
    """
    cmd = [
        "docker", "exec", "-i", "postgres-source",
        "psql", "-U", "admin", "-d", "app_db", "-t", "-A", "-c", sql_query
    ]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and result.stderr:
        print(f"❌ SQL Error: {result.stderr.strip()}")
    return result.stdout.strip()

def execute_psql_stdin(sql_script):
    """
    Thực thi chuỗi SQL lớn qua STDIN
    """
    cmd = [
        "docker", "exec", "-i", "postgres-source",
        "psql", "-U", "admin", "-d", "app_db", "-f", "-"
    ]
    result = subprocess.run(cmd, input=sql_script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and result.stderr:
        print(f"❌ SQL Stdin Error: {result.stderr.strip()}")
    return result.returncode == 0

def extract_id(output_str):
    for line in output_str.splitlines():
        line = line.strip()
        if line.isdigit():
            return int(line)
    return None

def generate_random_user():
    ho = random.choice(HO)
    dem = random.choice(TEN_DEM)
    ten = random.choice(TEN)
    full_name = f"{ho} {dem} {ten}"
    email_user = remove_accents(f"{ten}_{dem}_{random.randint(100, 99999)}")
    email = f"{email_user}@smartlife.vn"
    return full_name, email

def get_random_timestamp_in_day(target_date):
    """
    Sinh timestamp ngẫu nhiên trong ngày với phân bổ tập trung vào giờ cao điểm
    Peak 1: 08:00 - 12:00, Peak 2: 19:00 - 22:00
    """
    peak_choice = random.random()
    if peak_choice < 0.45:
        hour = random.randint(8, 12)
    elif peak_choice < 0.85:
        hour = random.randint(19, 22)
    else:
        hour = random.randint(0, 23)

    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    dt = datetime(target_date.year, target_date.month, target_date.day, hour, minute, second)
    return int(dt.timestamp() * 1000)

# ==============================================================================
# 1. GENERATOR: HISTORICAL POSTGRESQL DATABASE
# ==============================================================================
def seed_historical_database(start_date, end_date, output_dir=None):
    """
    Sinh toàn bộ dữ liệu người dùng, thiết bị, gói cước từ start_date đến end_date
    """
    print(f"\n=======================================================")
    print(f"🏛️ 1. SEEDING POSTGRESQL SOURCE (Từ {start_date} đến {end_date})")
    print(f"=======================================================")

    delta_days = (end_date - start_date).days + 1
    print(f"📅 Tổng số ngày cần sinh: {delta_days} ngày")

    all_users = []
    all_devices = []
    all_bills = []
    
    current_user_id = 1
    current_device_id = 1
    current_bill_id = 1

    active_subscriptions = []

    for day_idx in range(delta_days):
        curr_date = start_date + timedelta(days=day_idx)
        
        # Đường cong tăng trưởng (Growth Curve)
        progress = day_idx / max(1, delta_days - 1)
        users_today_count = int(random.randint(3, 8) + (progress * random.randint(10, 25)))

        # 1. Tạo Users & Devices mới trong ngày hôm nay
        for _ in range(users_today_count):
            name, email = generate_random_user()
            user_ts = get_random_timestamp_in_day(curr_date)
            user_rec = (current_user_id, name, email, user_ts)
            all_users.append(user_rec)
            u_id = current_user_id
            current_user_id += 1

            num_devs = random.choice([1, 1, 2])
            for _ in range(num_devs):
                dev_name = f"{random.choice(DEVICE_PREFIXES)} ({random.randint(1, 99)})"
                dev_type = random.choice(DEVICE_TYPES)
                dev_status = random.choices(["online", "offline"], weights=[0.88, 0.12])[0]
                dev_ts = user_ts + random.randint(1000, 30000)
                
                dev_rec = (current_device_id, dev_name, dev_type, dev_status, u_id, dev_ts)
                all_devices.append(dev_rec)
                d_id = current_device_id
                current_device_id += 1

                # Hóa đơn khởi tạo ban đầu
                plan = random.choice(PLAN_TYPES)
                amount = random.choice(PLAN_PRICES[plan])
                bill_status = random.choices(["paid", "pending"], weights=[0.94, 0.06])[0]
                bill_ts = dev_ts + random.randint(1000, 10000)
                
                bill_rec = (current_bill_id, u_id, d_id, amount, plan, bill_status, bill_ts)
                all_bills.append(bill_rec)
                current_bill_id += 1

                # Đăng ký chu kỳ gia hạn
                if plan == "monthly":
                    next_date = curr_date + timedelta(days=30)
                elif plan == "quarterly":
                    next_date = curr_date + timedelta(days=90)
                else:
                    next_date = curr_date + timedelta(days=365)

                if bill_status == "paid" and next_date <= end_date:
                    active_subscriptions.append({
                        "user_id": u_id,
                        "device_id": d_id,
                        "plan": plan,
                        "amount": amount,
                        "next_date": next_date
                    })

        # 2. Xử lý gia hạn cước cho các thuê bao đến hạn trong ngày curr_date
        renewed_subs = []
        for sub in active_subscriptions:
            if sub["next_date"] == curr_date:
                sub_ts = get_random_timestamp_in_day(curr_date)
                sub_bill_status = random.choices(["paid", "pending"], weights=[0.96, 0.04])[0]
                
                bill_rec = (current_bill_id, sub["user_id"], sub["device_id"], sub["amount"], sub["plan"], sub_bill_status, sub_ts)
                all_bills.append(bill_rec)
                current_bill_id += 1

                if sub["plan"] == "monthly":
                    sub["next_date"] = curr_date + timedelta(days=30)
                elif sub["plan"] == "quarterly":
                    sub["next_date"] = curr_date + timedelta(days=90)
                else:
                    sub["next_date"] = curr_date + timedelta(days=365)

                if sub["next_date"] <= end_date:
                    renewed_subs.append(sub)
            elif sub["next_date"] > curr_date:
                renewed_subs.append(sub)
        
        active_subscriptions = renewed_subs

    print(f"📊 Đã chuẩn bị: {len(all_users):,} Users | {len(all_devices):,} Devices | {len(all_bills):,} Bills")
    print(f"🚀 Đang nạp Batch dữ liệu vào PostgreSQL Source...")

    init_sql = """
    CREATE TABLE IF NOT EXISTS public.users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255),
        updated_at BIGINT
    );
    CREATE TABLE IF NOT EXISTS public.devices (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        device_type VARCHAR(100),
        status VARCHAR(50),
        owner_id INT,
        updated_at BIGINT
    );
    CREATE TABLE IF NOT EXISTS public.cam_bills (
        id SERIAL PRIMARY KEY,
        user_id INT,
        device_id INT,
        amount NUMERIC(15, 2),
        plan_type VARCHAR(50),
        status VARCHAR(50),
        updated_at BIGINT
    );
    """
    execute_psql(init_sql)

    # Batch Insert Users
    chunk_size = 5000
    for i in range(0, len(all_users), chunk_size):
        chunk = all_users[i:i + chunk_size]
        vals = ", ".join([f"({r[0]}, '{r[1]}', '{r[2]}', {r[3]})" for r in chunk])
        sql = f"INSERT INTO public.users (id, name, email, updated_at) VALUES {vals} ON CONFLICT (id) DO NOTHING;"
        execute_psql_stdin(sql)

    # Batch Insert Devices
    for i in range(0, len(all_devices), chunk_size):
        chunk = all_devices[i:i + chunk_size]
        vals = ", ".join([f"({r[0]}, '{r[1]}', '{r[2]}', '{r[3]}', {r[4]}, {r[5]})" for r in chunk])
        sql = f"INSERT INTO public.devices (id, name, device_type, status, owner_id, updated_at) VALUES {vals} ON CONFLICT (id) DO NOTHING;"
        execute_psql_stdin(sql)

    # Batch Insert Bills
    for i in range(0, len(all_bills), chunk_size):
        chunk = all_bills[i:i + chunk_size]
        vals = ", ".join([f"({r[0]}, {r[1]}, {r[2]}, {r[3]}, '{r[4]}', '{r[5]}', {r[6]})" for r in chunk])
        sql = f"INSERT INTO public.cam_bills (id, user_id, device_id, amount, plan_type, status, updated_at) VALUES {vals} ON CONFLICT (id) DO NOTHING;"
        execute_psql_stdin(sql)

    # Cập nhật lại SEQUENCE ID
    execute_psql(f"SELECT setval('public.users_id_seq', (SELECT max(id) FROM public.users));")
    execute_psql(f"SELECT setval('public.devices_id_seq', (SELECT max(id) FROM public.devices));")
    execute_psql(f"SELECT setval('public.cam_bills_id_seq', (SELECT max(id) FROM public.cam_bills));")

    print(f"✅ Hoàn thành Seeding PostgreSQL Source thành công!")
    return all_users, all_devices, all_bills

# ==============================================================================
# 2. GENERATOR: HISTORICAL IOT CAMERA LOGS
# ==============================================================================
def generate_historical_camera_logs(devices, start_date, end_date, output_dir, logs_per_day=300):
    """
    Sinh các file log sự kiện camera theo từng ngày/tháng
    """
    print(f"\n=======================================================")
    print(f"📄 2. GENERATING IOT CAMERA LOGS (s3://source-logs-bucket/)")
    print(f"=======================================================")

    logs_dir = os.path.join(output_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    delta_days = (end_date - start_date).days + 1
    device_ids = [d[0] for d in devices] if devices else list(range(1, 100))

    total_logs_count = 0

    for day_idx in range(delta_days):
        curr_date = start_date + timedelta(days=day_idx)
        date_str = curr_date.strftime("%Y%m%d")

        month_dir = os.path.join(logs_dir, f"year={curr_date.year}", f"month={curr_date.strftime('%m')}")
        os.makedirs(month_dir, exist_ok=True)
        file_path = os.path.join(month_dir, f"camera_logs_{date_str}.json")

        daily_count = random.randint(int(logs_per_day * 0.7), int(logs_per_day * 1.3))
        lines = []

        for _ in range(daily_count):
            dev_id = random.choice(device_ids)
            event_type = random.choices(
                ["motion_detected", "human_detected", "heartbeat", "connection_lost", "stream_started", "sd_card_error"],
                weights=[0.35, 0.20, 0.25, 0.08, 0.10, 0.02]
            )[0]

            severity = "INFO"
            err_code = None
            if event_type == "connection_lost":
                severity = "ERROR"
                err_code = random.choice(["ERR_SOCKET_TIMEOUT", "ERR_DHCP_FAILED", "ERR_SIGNAL_WEAK"])
            elif event_type == "sd_card_error":
                severity = "WARN"
                err_code = random.choice(["ERR_SD_CARD_FULL", "ERR_WRITE_FAILED"])

            ts_ms = get_random_timestamp_in_day(curr_date)
            ip_sub = random.randint(1, 254)
            ip = f"192.168.1.{ip_sub}"

            log_entry = {
                "event_id": f"evt-{uuid.uuid4().hex[:12]}",
                "timestamp": ts_ms,
                "device_id": dev_id,
                "event_type": event_type,
                "severity": severity,
                "firmware_version": random.choice(FIRMWARE_VERSIONS),
                "ip_address": ip,
                "fps": random.choice([15, 25, 30]),
                "bitrate_kbps": random.choice([1024, 2048, 4096]),
                "duration_sec": random.randint(10, 60) if "detected" in event_type else 0,
                "error_code": err_code
            }
            lines.append(json.dumps(log_entry))

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

        total_logs_count += daily_count

    print(f"✅ Đã tạo {delta_days} file Log (Tổng cộng: {total_logs_count:,} events) tại: {logs_dir}")

# ==============================================================================
# 3. GENERATOR: HISTORICAL PAYMENT RECONCILIATION CSV FILES
# ==============================================================================
def generate_historical_reconciliation_files(bills, start_date, end_date, output_dir):
    """
    Sinh các file CSV đối soát thanh toán định kỳ từng ngày
    """
    print(f"\n=======================================================")
    print(f"📁 3. GENERATING PAYMENT RECONCILIATION CSV (s3://source-files-bucket/)")
    print(f"=======================================================")

    files_dir = os.path.join(output_dir, "files")
    os.makedirs(files_dir, exist_ok=True)

    bills_by_date = {}
    for bill in bills:
        dt = datetime.fromtimestamp(bill[6] / 1000.0)
        d_key = dt.date()
        if d_key not in bills_by_date:
            bills_by_date[d_key] = []
        bills_by_date[d_key].append(bill)

    delta_days = (end_date - start_date).days + 1
    total_csv_rows = 0

    for day_idx in range(delta_days):
        curr_date = start_date + timedelta(days=day_idx)
        date_str = curr_date.strftime("%Y%m%d")
        file_path = os.path.join(files_dir, f"payment_reconciliation_{date_str}.csv")

        day_bills = bills_by_date.get(curr_date, [])
        header = "partner_trans_id,bill_id,user_id,payment_gateway,trans_amount,partner_fee,trans_time,trans_status,bank_code\n"

        rows = [header]
        for b in day_bills:
            b_id, u_id, d_id, amt, plan, status, ts_ms = b
            dt_str = datetime.fromtimestamp(ts_ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
            
            gateway = random.choice(PAYMENT_GATEWAYS)
            fee = round(amt * 0.011, 2)
            bank = random.choice(BANK_CODES)
            
            p_status = "SUCCESS" if status == "paid" else "PENDING"
            if random.random() < 0.02:
                p_status = "REFUNDED"

            trans_id = f"{gateway.upper()[:3]}_{date_str}_{b_id:06d}"
            row = f"{trans_id},{b_id},{u_id},{gateway},{amt:.2f},{fee:.2f},{dt_str},{p_status},{bank}\n"
            rows.append(row)

        with open(file_path, "w", encoding="utf-8") as f:
            f.writelines(rows)

        total_csv_rows += len(day_bills)

    print(f"✅ Đã tạo {delta_days} file CSV đối soát (Tổng cộng: {total_csv_rows:,} dòng) tại: {files_dir}")

# ==============================================================================
# 4. CHẾ ĐỘ STREAMING & BATCH SINGLE RECORD (KẾ THỪA CŨ)
# ==============================================================================
def seed_single_transaction():
    name, email = generate_random_user()
    now_ms = int(time.time() * 1000)

    sql_user = f"""
    INSERT INTO public.users (name, email, updated_at) 
    VALUES ('{name}', '{email}', {now_ms}) RETURNING id;
    """
    user_id = extract_id(execute_psql(sql_user))
    if not user_id:
        return

    num_devices = random.choice([1, 1, 2])
    created_devices = 0
    for _ in range(num_devices):
        dev_name = f"{random.choice(DEVICE_PREFIXES)} ({random.randint(1, 99)})"
        dev_type = random.choice(DEVICE_TYPES)
        dev_status = random.choices(["online", "offline"], weights=[0.85, 0.15])[0]
        
        sql_dev = f"""
        INSERT INTO public.devices (name, device_type, status, owner_id, updated_at)
        VALUES ('{dev_name}', '{dev_type}', '{dev_status}', {user_id}, {now_ms}) RETURNING id;
        """
        device_id = extract_id(execute_psql(sql_dev))
        if device_id:
            created_devices += 1
            plan = random.choice(PLAN_TYPES)
            amount = random.choice(PLAN_PRICES[plan])
            bill_status = random.choices(["paid", "pending"], weights=[0.9, 0.1])[0]

            sql_bill = f"""
            INSERT INTO public.cam_bills (user_id, device_id, amount, plan_type, status, updated_at)
            VALUES ({user_id}, {device_id}, {amount}, '{plan}', '{bill_status}', {now_ms});
            """
            execute_psql(sql_bill)

    print(f"✨ [INSERT CDC] Đã tạo User #{user_id} ({name}) + {created_devices} Camera + Bill")

def trigger_random_update():
    now_ms = int(time.time() * 1000)
    choice = random.choice(["toggle_device", "pay_bill", "update_user"])

    if choice == "toggle_device":
        sql = f"""
        UPDATE public.devices 
        SET status = CASE WHEN status = 'online' THEN 'offline' ELSE 'online' END,
            updated_at = {now_ms}
        WHERE id = (SELECT id FROM public.devices ORDER BY RANDOM() LIMIT 1)
        RETURNING id, name, status;
        """
        res = execute_psql(sql)
        if res:
            first_line = res.splitlines()[0] if res.splitlines() else res
            print(f"🔄 [UPDATE CDC] Thiết bị thay đổi trạng thái: {first_line}")

    elif choice == "pay_bill":
        sql = f"""
        UPDATE public.cam_bills
        SET status = 'paid', updated_at = {now_ms}
        WHERE id = (SELECT id FROM public.cam_bills WHERE status = 'pending' ORDER BY RANDOM() LIMIT 1)
        RETURNING id, amount, status;
        """
        res = execute_psql(sql)
        if res:
            first_line = res.splitlines()[0] if res.splitlines() else res
            print(f"💰 [UPDATE CDC] Hóa đơn đã được thanh toán: {first_line}")

    elif choice == "update_user":
        vip_tag = random.choice(["(VIP)", "(Pro)", "(Gold)", "(Diamond)"])
        sql = f"""
        UPDATE public.users
        SET name = name || ' {vip_tag}', updated_at = {now_ms}
        WHERE id = (SELECT id FROM public.users WHERE name NOT LIKE '%(%' ORDER BY RANDOM() LIMIT 1)
        RETURNING id, name;
        """
        res = execute_psql(sql)
        if res:
            first_line = res.splitlines()[0] if res.splitlines() else res
            print(f"👤 [UPDATE CDC] Cập nhật thông tin User: {first_line}")

def reset_database():
    print("🧹 Đang dọn dẹp và reset dữ liệu bảng PostgreSQL...")
    sql = """
    TRUNCATE TABLE public.cam_bills, public.devices, public.users RESTART IDENTITY CASCADE;
    """
    execute_psql(sql)
    print("✅ Đã reset sạch sẽ các bảng về ID 1!")

# ==============================================================================
# MAIN ENTRYPOINT
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Enterprise Multi-Source Data Seeder & Historical Generator")
    parser.add_argument("--from-start-of-year", action="store_true", help="Sinh toàn bộ dữ liệu lịch sử từ 01/01 đến nay (~8 tháng)")
    parser.add_argument("--days", type=int, default=None, help="Số ngày lịch sử cần sinh tính lùi từ hôm nay (ví dụ: --days 30)")
    parser.add_argument("--start-date", type=str, default=None, help="Ngày bắt đầu (Định dạng YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="Ngày kết thúc (Định dạng YYYY-MM-DD)")
    parser.add_argument("--output-dir", type=str, default="./data_lake_source", help="Thư mục lưu trữ file Logs và CSV đối soát")
    parser.add_argument("--logs-per-day", type=int, default=300, help="Số lượng log events trung bình mỗi ngày (Mặc định: 300)")
    
    # Cờ tùy chọn từng nguồn dữ liệu
    parser.add_argument("--skip-db", action="store_true", help="Bỏ qua nạp vào PostgreSQL (chỉ sinh file Logs và CSV, thích hợp cho Option 2)")
    parser.add_argument("--only-db", action="store_true", help="Chỉ sinh và nạp vào PostgreSQL")
    parser.add_argument("--only-logs", action="store_true", help="Chỉ sinh file IoT Camera Logs")
    parser.add_argument("--only-files", action="store_true", help="Chỉ sinh file Payment Reconciliation CSV")

    args = parser.parse_args()

    print("\n=======================================================")
    print("🚀 ENTERPRISE DATA SEEDER & HISTORICAL GENERATOR")
    print("=======================================================")

    if args.reset and not args.skip_db and not args.only_logs and not args.only_files:
        reset_database()

    today = date.today()

    if args.from_start_of_year or args.days or args.start_date:
        if args.start_date:
            s_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        elif args.from_start_of_year:
            s_date = date(today.year, 1, 1)
        elif args.days:
            s_date = today - timedelta(days=args.days - 1)
        else:
            s_date = date(today.year, 1, 1)

        e_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else today

        print(f"🎯 Kích hoạt chế độ sinh dữ liệu Lịch sử Đa nguồn từ {s_date} đến {e_date}")
        
        # 1. Sinh DB Postgres (nếu không bị skip)
        users, devices, bills = [], [], []
        if not args.skip_db and not args.only_logs and not args.only_files:
            users, devices, bills = seed_historical_database(s_date, e_date, args.output_dir)
        elif args.skip_db or args.only_logs or args.only_files:
            # Tạo mock list nhẹ trong bộ nhớ nếu không ghi vào Postgres
            print("ℹ️ Bỏ qua ghi vào PostgreSQL (chỉ sinh dữ liệu tệp)")
            devices = [(i, f"Device_{i}") for i in range(1, 200)]
            bills = [(i, i % 50 + 1, i, 150000.0, "monthly", "paid", int(time.time() * 1000)) for i in range(1, 500)]

        # 2. Sinh IoT Camera Logs
        if not args.only_db and not args.only_files:
            generate_historical_camera_logs(devices, s_date, e_date, args.output_dir, args.logs_per_day)

        # 3. Sinh Payment Reconciliation CSVs
        if not args.only_db and not args.only_logs:
            generate_historical_reconciliation_files(bills, s_date, e_date, args.output_dir)

        print("\n=======================================================")
        print("🎉 HOÀN THÀNH TẤT CẢ CÁC NGUỒN DỮ LIỆU ĐÃ CHỌN!")
        print(f"📁 Thư mục đầu ra Files & Logs: {os.path.abspath(args.output_dir)}")
        print("=======================================================\n")

    elif args.stream:
        print(f"📡 Đang chạy chế độ Streaming liên tục (Mỗi {args.interval}s một sự kiện)... Nhấn Ctrl+C để dừng.\n")
        count = 0
        try:
            while True:
                action = random.choices(["insert", "update"], weights=[0.75, 0.25])[0]
                if action == "insert":
                    seed_single_transaction()
                else:
                    trigger_random_update()
                
                count += 1
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print(f"\n⏹️ Đã dừng seeder. Tổng cộng đã phát sinh {count} sự kiện CDC.")
    else:
        print(f"📦 Đang tạo {args.count} khách hàng & thiết bị theo chế độ Batch hiện tại...\n")
        for i in range(args.count):
            seed_single_transaction()
            time.sleep(0.02)
        print(f"\n🎉 Hoàn thành Batch Seeding {args.count} khách hàng & thiết bị!")

if __name__ == "__main__":
    main()

