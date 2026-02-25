# wordpress | ecommerce
# SQL to Excel RFM

ابزار پایتون برای خواندن دامپ SQL (مثلاً وردپرس/ووکامرس)، تبدیل به دیتابیس SQLite، استخراج داده‌های مشتری و سفارش، و خروجی Excel به‌همراه تحلیل RFM و نمودارها.

---

## قابلیت‌ها

- **خواندن دامپ SQL**: پشتیبانی از فایل‌های `.sql`، `.gz` و `.sql.gz`
- **تشخیص پیشوند جداول**: تشخیص خودکار پیشوند (مثل `wp_`) و گروه‌های جدول
- **خروجی Excel**: جداول/ویوهای `customer_purchases`، `user_full_data`، `rfm_data` با فرمت عددی (کاما) برای مبالغ
- **تحلیل RFM**: محاسبه Recency، Frequency، Monetary و باندهای Quantile؛ تولید فایل `rfm_constant.xlsx` و `rfm_scores.xlsx` با ستون سگمنت
- **نمودارها**: در حالت «استفاده از دادهٔ موجود» تولید ۷ نمودار (هیت‌مپ، بار، اسکتر، تری‌مپ و...) در پوشه `charts`
- **دو حالت اجرا**: وارد کردن دادهٔ جدید از دامپ، یا انتخاب یک پوشهٔ خروجی قبلی برای محاسبه امتیاز RFM و نمودارها

---

## پیش‌نیازها

- Python 3.10+
- وابستگی‌ها در `requirements.txt`

---

## نصب

```bash
git clone <repository-url>
cd sql-to-excel-tool
python -m venv .venv
.venv\Scripts\activate   # ویندوز
# یا: source .venv/bin/activate   # لینوکس/مک
pip install -r requirements.txt
```

---

## ساختار پروژه

```
sql-to-excel-tool/
├── main.py              # نقطه ورود و منوی اصلی
├── flows.py             # جریان‌های «داده جدید» و «داده موجود»
├── config.py           # مسیرها و تنظیمات (dump, output, db, TABLE_GROUPS)
├── requirements.txt
├── dump/                # قرار دادن فایل‌های دامپ SQL اینجا
├── output/              # پوشه‌های خروجی (مثلاً amir2_1، amir2_2)
├── db/                  # دیتابیس موقت SQLite (converted.db)
├── core/                # ماژول‌های اصلی
│   ├── dump_reader.py   # خواندن و بررسی دامپ
│   ├── importer.py     # وارد کردن به SQLite
│   ├── db_manager.py   # مدیریت دیتابیس
│   ├── customer_purchases.py
│   ├── user_full_data.py
│   ├── rfm_data.py     # جدول/ویوی RFM
│   ├── rfm_constants.py # ساخت rfm_constant.xlsx
│   ├── rfm_charts.py   # ساخت نمودارها
│   └── excel_exporter.py
└── utils/
    └── helpers.py      # توابع کمکی (پوشه خروجی، README، encoding و...)
```

---

## نحوه استفاده

### اجرای برنامه

```bash
python main.py
```

در منو دو گزینه اصلی دارید:

1. **وارد کردن داده‌های جدید**  
   - پاک شدن دیتابیس موقت  
   - انتخاب فایل دامپ از پوشه `dump`  
   - انتخاب مبنای محاسبه RFM: از ابتدای تراکنش‌ها یا یک تاریخ شمسی  
   - وارد کردن جداول به SQLite، ساخت ویوها و جداول تحلیلی  
   - ساخت پوشه خروجی (مثلاً `output/amir2_1`) و خروجی‌های Excel  
   - کپی `converted.db` به پوشه خروجی  

2. **استفاده از داده‌های وارد شده**  
   - لیست پوشه‌های داخل `output` (با تاریخ و تعداد فایل)  
   - انتخاب یک پوشه  
   - بررسی وجود و صحت `1_rfm_data.xlsx` و `rfm_constant.xlsx`  
   - در صورت تأیید: ساخت `rfm_scores.xlsx` و نمودارها در `charts/`  

---

## فایل‌های خروجی

در هر پوشه خروجی (مثلاً `output/amir2_1/`) معمولاً موارد زیر تولید می‌شوند:

| فایل | توضیح |
|------|--------|
| `1_user_orders.xlsx` | اطلاعات خرید مشتری (سفارش‌ها) |
| `1_user_full_data.xlsx` | دادهٔ تلفیقی کاربران |
| `1_rfm_data.xlsx` | Recency، Frequency، Monetary و آمار مرتبط |
| `rfm_constant.xlsx` | باندهای Quantile و قواعد سگمنت (برای انسان و مرحله بعد) |
| `rfm_scores.xlsx` | امتیاز R/F/M، `rfm_score` و **ستون سگمنت** برای هر کاربر |
| `converted.db` | کپی دیتابیس SQLite استفاده‌شده |
| `README.txt` | تاریخ گزارش، نام/حجم دامپ، لیست فایل‌های اکسل و نمودارها |
| `charts/*.png` | ۷ نمودار (هیت‌مپ R-F، اندازه سگمنت، اسکتر، درآمد به سگمنت، توزیع At Risk، CLV vs RFM، تری‌مپ سگمنت‌ها) |

اگر تعداد ردیف‌ها از حد مجاز بیشتر شود، فایل‌های بعدی با پیشوند شماره (مثلاً `2_rfm_data.xlsx`) ساخته می‌شوند.

---

## تحلیل RFM

- **Recency**: روزهای گذشته از آخرین خرید  
- **Frequency**: تعداد سفارش‌ها  
- **Monetary**: مجموع مبلغ خرید  

امتیازدهی بر اساس فایل `rfm_constant.xlsx` (باندهای Quantile) انجام می‌شود. سگمنت‌ها (مثل Champions، At Risk، Loyal و...) در ستون `segment` فایل `rfm_scores.xlsx` قرار می‌گیرند تا بتوانید مشتریان هر بخش از نمودارها را با فیلتر کردن این ستون پیدا کنید.

---

## تنظیمات

در `config.py` می‌توانید تغییر دهید:

- `DUMP_DIR`, `OUTPUT_DIR`, `DB_DIR`: مسیر پوشه‌های دامپ، خروجی و دیتابیس  
- `EXCEL_MAX_ROWS_PER_FILE`: حداکثر ردیف در هر فایل Excel (پیش‌فرض ۵۰۰٬۰۰۰)  
- `RFM_QUANTILE_BANDS`: تعداد باند Quantile برای RFM (پیش‌فرض ۵)  
- `TABLE_GROUPS`: گروه‌های جدول مورد انتظار برای تشخیص دامپ (مثلاً `wp`, `avanse`)  

---

## لایسنس

استفاده آزاد در پروژهٔ شخصی یا داخلی. برای استفادهٔ تجاری یا توزیع مجدد شرایط پروژه را رعایت کنید.
