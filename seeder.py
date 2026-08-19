#!/usr/bin/env python3
"""
==============================================================================
AUTOMATIC DATABASE SEEDER FOR REAL-TIME CDC & LAKEHOUSE BENCHMARK
==============================================================================
Tạo dữ liệu giả lập thực tế tự động cho hệ thống:
- Người dùng (users)
- Thiết bị camera thông minh (devices)
- Hóa đơn và gói cước (cam_bills)
- Hỗ trợ cả 2 chế độ: Batch Seed (tạo số lượng lớn) và Continuous Stream (giả lập traffic thời gian thực)
==============================================================================
"""

import sys
import time
import random
import subprocess
import argparse
from datetime import datetime

# Danh sách họ & tên đệm & tên tiếng Việt
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
    email_user = remove_accents(f"{ten}_{dem}_{random.randint(100, 9999)}")
    email = f"{email_user}@smartlife.vn"
    return full_name, email

def seed_single_transaction():
    """
    Tạo 1 người dùng mới kèm 1-2 thiết bị và hóa đơn
    """
    name, email = generate_random_user()
    now_ms = int(time.time() * 1000)

    # 1. Insert User
    sql_user = f"""
    INSERT INTO public.users (name, email, updated_at) 
    VALUES ('{name}', '{email}', {now_ms}) RETURNING id;
    """
    user_id = extract_id(execute_psql(sql_user))
    if not user_id:
        return

    # 2. Insert 1-2 Devices
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
            # 3. Insert Cam Bill
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
    """
    Giả lập sự kiện UPDATE (Thay đổi trạng thái camera, thanh toán hóa đơn pending, đổi tên user)
    """
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
    print("🧹 Đang dọn dẹp và reset dữ liệu bảng...")
    sql = """
    TRUNCATE TABLE public.cam_bills, public.devices, public.users RESTART IDENTITY CASCADE;
    """
    execute_psql(sql)
    print("✅ Đã reset sạch sẽ các bảng về ID 1!")

def main():
    parser = argparse.ArgumentParser(description="Tự động Seed dữ liệu cho Postgres Source CDC")
    parser.add_argument("--count", type=int, default=10, help="Số lượng giao dịch cần tạo ở chế độ Batch (Mặc định: 10)")
    parser.add_argument("--stream", action="store_true", help="Bật chế độ Continuous Streaming liên tục theo thời gian thực")
    parser.add_argument("--interval", type=float, default=2.0, help="Khoảng cách giữa các đợt phát sinh dữ liệu (giây)")
    parser.add_argument("--reset", action="store_true", help="Xóa sạch dữ liệu cũ trước khi seed mới")

    args = parser.parse_args()

    print("\n=======================================================")
    print("🚀 AUTOMATIC POSTGRESQL CDC DATA SEEDER")
    print("=======================================================")

    if args.reset:
        reset_database()

    if args.stream:
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
        print(f"📦 Đang tạo {args.count} khách hàng & thiết bị theo chế độ Batch...\n")
        for i in range(args.count):
            seed_single_transaction()
            time.sleep(0.02)
        print(f"\n🎉 Hoàn thành Batch Seeding {args.count} khách hàng & thiết bị!")

if __name__ == "__main__":
    main()
