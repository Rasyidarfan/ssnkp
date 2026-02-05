
import streamlit as st
import sqlite3
import json
import re
import pandas as pd

DB_PATH = "susenas.db"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_rekap(kab, nks, nurt):
    """Load rekap row + parse data_json."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM rekap WHERE Kab = ? AND NKS = ? AND Nurt = ?", (kab, nks, nurt))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    rekap = dict(row)
    if rekap.get("data_json"):
        try:
            rekap.update(json.loads(rekap["data_json"]))
        except (json.JSONDecodeError, TypeError):
            pass
    return rekap


def load_komoditi(kab, nks, nurt):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT mk.*, k.Rincian, k.KodeCOICOP
        FROM modul_komoditi mk
        LEFT JOIN komoditi k ON mk.NoUrutKomoditiFK = k.NoUrut
        WHERE mk.Kab = ? AND mk.NKS = ? AND mk.Nurt = ?
        ORDER BY mk.NoUrutKomoditiFK
        """,
        (kab, nks, nurt),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def load_pendapatan_5a(kab, nks, nurt):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pendapatan_5a WHERE Kab = ? AND NKS = ? AND Nurt = ?", (kab, nks, nurt))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def load_pendapatan_5b(kab, nks, nurt):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pendapatan_5b WHERE Kab = ? AND NKS = ? AND Nurt = ?", (kab, nks, nurt))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def fmt_currency(val):
    """Format angka sebagai mata uang Indonesia (tanpa simbol)."""
    if val is None:
        return "-"
    try:
        return f"{float(val):,.0f}"
    except (TypeError, ValueError):
        return str(val)


def fmt_number(val):
    if val is None:
        return "-"
    try:
        v = float(val)
        return f"{v:,.0f}" if v == int(v) else f"{v:,.2f}"
    except (TypeError, ValueError):
        return str(val)


def val_or_dash(val):
    if val is None or val == "":
        return "-"
    return str(val)


# ---------------------------------------------------------------------------
# Komoditi split logic (sama seperti app.js)
# ---------------------------------------------------------------------------
def has_any_value(item):
    for i in range(1, 15):
        if item.get(f"Kolom{i}") is not None:
            return True
    return False


def split_komoditi(komoditi_rows):
    """Split komoditi menjadi makanan / individu / non-makanan."""
    valid = [k for k in komoditi_rows if has_any_value(k)]
    makanan = [k for k in valid if (k.get("NoUrutKomoditiFK") or 0) < 186]
    individu = [k for k in valid if 186 <= (k.get("NoUrutKomoditiFK") or 0) < 226]
    non_makanan = [k for k in valid if (k.get("NoUrutKomoditiFK") or 0) >= 226]

    # Sort individu by IDART then NoUrutKomoditiFK
    individu.sort(key=lambda x: (x.get("IDART") or "", x.get("NoUrutKomoditiFK") or 0))

    return makanan, individu, non_makanan


def build_idart_map(individu):
    """Map IDART -> sequential ART number (1, 2, 3 …)."""
    seen = []
    for item in individu:
        idart = item.get("IDART")
        if idart and idart not in seen:
            seen.append(idart)
    return {idart: idx + 1 for idx, idart in enumerate(seen)}


def lainnya_str(item):
    """Kolom 7-14 yang tidak None -> string K7:val, K8:val …"""
    parts = []
    for i in range(7, 15):
        val = item.get(f"Kolom{i}")
        if val is not None:
            parts.append(f"K{i}:{fmt_number(val)}")
    return ", ".join(parts) if parts else "-"


# ---------------------------------------------------------------------------
# Blok Rekap parser (sama logika seperti renderBlokRekap di app.js)
# ---------------------------------------------------------------------------
BLOK_TITLES = {
    "B431": "Blok IV.3.1 - Pengeluaran",
    "B432": "Blok IV.3.2 - Pengeluaran Makanan Seminggu",
    "B433": "Blok IV.3.3 - Pengeluaran Non Makanan",
    "B5A": "Blok V.A - Jumlah Pendapatan Pekerjaan",
    "B5B": "Blok V.B - Jumlah Pendapatan Usaha",
    "B5C": "Blok V.C - Pendapatan dari Sumber Lain",
    "B5D": "Blok V.D - Penerimaan",
    "B5E": "Blok V.E - Penerimaan Lainnya",
    "B5F": "Blok V.F - Pengiriman Uang/Barang",
    "B5G": "Blok V.G - Pengeluaran Bukan Konsumsi",
    "B6": "Blok VI - Pengeluaran dan Pendapatan",
    "B7": "Blok VII - Pendapatan dan Pengeluaran",
}

# Total-rows per blok (row numbers yang di-highlight)
TOTAL_ROWS = {
    "B432": {"15", "16"},
    "B433": {"15", "16"},
    "B6": {"7", "8"},
    "B7": {"7", "8"},
    "B5E": {"6"},
    "B5F": {"6"},
    "B5G": {"6"},
}

# Sub-rows yang perlu indent (R11-R15 → R1.1-R1.5)
SUB_ROW_BLOKS = {"B5E", "B5G"}
SUB_ROWS = {"11", "12", "13", "14", "15"}


def classify_blok_key(key):
    """Classifiy key seperti B432R1K3 -> blok='B432'. Return None kalau tidak match."""
    if re.match(r"^B43[123]", key):
        return key[:4]
    if re.match(r"^B5[A-G]R", key):
        return key[:3]
    if re.match(r"^B5[A-G]K", key):          # misal B5AK5J
        return key[:3]
    if re.match(r"^B6R", key):
        return "B6"
    if re.match(r"^B7R", key):
        return "B7"
    return None


def parse_blok_data(rekap):
    """Dari rekap dict, kumpulkan semua blok -> {blok_name: {row: {col: value}}}."""
    blok_raw = {}  # blok_name -> {key: value}
    for key, value in rekap.items():
        blok_name = classify_blok_key(key)
        if blok_name:
            blok_raw.setdefault(blok_name, {})[key] = value

    # Parse menjadi rows & columns
    blok_tables = {}  # blok_name -> {row_str: {col_str: value}}
    for blok_name, data in blok_raw.items():
        # Skip kalau semua value None
        if not any(v is not None for v in data.values()):
            continue

        rows = {}
        for key, value in data.items():
            # Strip prefix blok_name
            suffix = key[len(blok_name):]

            # Pattern: R<num>K<num>  misal R1K3
            m = re.match(r"R(\d+[A-Z]?)K(\d+)", suffix)
            if m:
                row_num, col_num = m.group(1), m.group(2)
                rows.setdefault(row_num, {})[f"K{col_num}"] = value
                continue

            # Pattern: R<num> tanpa kolom  misal R8 (B6R8)
            m = re.match(r"R(\d+)$", suffix)
            if m:
                row_num = m.group(1)
                rows.setdefault(row_num, {})["K4"] = value
                continue

            # Pattern: K<num>J  misal K5J (B5AK5J) -> row '1'
            m = re.match(r"K(\d+)", suffix)
            if m:
                col_num = m.group(1)
                rows.setdefault("1", {})[f"K{col_num}"] = value
                continue

        if rows:
            blok_tables[blok_name] = rows

    return blok_tables


def sort_rows(blok_name, row_keys):
    """Sort row keys. Untuk B5E/B5G, sub-rows 11-15 datang setelah row 1."""
    is_sub = blok_name in SUB_ROW_BLOKS

    def sort_key(r):
        # Strip trailing letters for numeric comparison
        num_str = re.match(r"(\d+)", r)
        num = int(num_str.group(1)) if num_str else 0
        if is_sub and r in SUB_ROWS:
            return 1.0 + (num - 10) / 10.0
        return float(num)

    return sorted(row_keys, key=sort_key)


def row_label(blok_name, row_num):
    if blok_name in SUB_ROW_BLOKS and row_num in SUB_ROWS:
        return f"R1.{int(row_num) - 10}"
    return f"R{row_num}"


# ---------------------------------------------------------------------------
# Rendering helpers for Streamlit
# ---------------------------------------------------------------------------
def render_identitas(rekap):
    """Blok Identitas Rumah Tangga."""
    jam = rekap.get("JamMulai")
    menit = rekap.get("MenitMulai")
    waktu = f"{jam}:{int(menit):02d}" if jam is not None else "-"

    data = {
        "Provinsi": val_or_dash(rekap.get("Prop")),
        "Kabupaten": val_or_dash(rekap.get("Kab")),
        "Kecamatan": val_or_dash(rekap.get("Kec")),
        "Desa": val_or_dash(rekap.get("Desa")),
        "Klasifikasi": val_or_dash(rekap.get("Klasifikasi")),
        "SLS": val_or_dash(rekap.get("SLS")),
        "NKS": val_or_dash(rekap.get("NKS")),
        "No. Urut": val_or_dash(rekap.get("Nurt")),
        "NBS": val_or_dash(rekap.get("NBS")),
        "NUBF": val_or_dash(rekap.get("NUBF")),
        "Nama KRT": val_or_dash(rekap.get("R110")),
        "Alamat": val_or_dash(rekap.get("Alamat")),
        "Semester": val_or_dash(rekap.get("Semester")),
        "Waktu Mulai": waktu,
        "Kode Pencacah": val_or_dash(rekap.get("KodePencacah")),
        "Kode Pengawas": val_or_dash(rekap.get("KodePengawas")),
    }

    # 2-kolom layout menggunakan grid
    col1, col2 = st.columns(2)
    keys = list(data.keys())
    # Pair up: (Provinsi, Kabupaten), (Kecamatan, Desa), dst.
    pairs = [
        ("Provinsi", "Kabupaten"),
        ("Kecamatan", "Desa"),
        ("Klasifikasi", "SLS"),
        ("NKS", "No. Urut"),
        ("NBS", "NUBF"),
        ("Nama KRT", None),
        ("Alamat", None),
        ("Semester", "Waktu Mulai"),
        ("Kode Pencacah", "Kode Pengawas"),
    ]
    for left_key, right_key in pairs:
        col1.markdown(f"**{left_key}:** {data[left_key]}")
        if right_key:
            col2.markdown(f"**{right_key}:** {data[right_key]}")
        else:
            col2.markdown("")


def render_komoditi_makanan(makanan):
    """Blok IV.1 - Pengeluaran Makanan."""
    if not makanan:
        st.info("Tidak ada data pengeluaran makanan.")
        return

    rows = []
    for item in makanan:
        rows.append({
            "No Urut": item.get("NoUrutKomoditiFK", "-"),
            "Komoditi": item.get("Rincian") or "-",
            "K1 Qty": fmt_number(item.get("Kolom1")),
            "K2 Nilai": fmt_currency(item.get("Kolom2")),
            "K3 Qty": fmt_number(item.get("Kolom3")),
            "K4 Nilai": fmt_currency(item.get("Kolom4")),
            "K5 Total Qty": fmt_number(item.get("Kolom5")),
            "K6 Total Nilai": fmt_currency(item.get("Kolom6")),
            "Lainnya": lainnya_str(item),
        })
    st.dataframe(pd.DataFrame(rows), hide_index=True)


def render_komoditi_individu(individu):
    """Blok IV.1 - Pengeluaran Makanan Individu."""
    if not individu:
        st.info("Tidak ada data pengeluaran makanan individu.")
        return

    idart_map = build_idart_map(individu)
    rows = []
    for item in individu:
        rows.append({
            "No Urut": item.get("NoUrutKomoditiFK", "-"),
            "ART": idart_map.get(item.get("IDART"), "-"),
            "Komoditi": item.get("Rincian") or "-",
            "K1 Qty": fmt_number(item.get("Kolom1")),
            "K2 Nilai": fmt_currency(item.get("Kolom2")),
            "K3 Qty": fmt_number(item.get("Kolom3")),
            "K4 Nilai": fmt_currency(item.get("Kolom4")),
            "K5 Total Qty": fmt_number(item.get("Kolom5")),
            "K6 Total Nilai": fmt_currency(item.get("Kolom6")),
            "Lainnya": lainnya_str(item),
        })
    st.dataframe(pd.DataFrame(rows), hide_index=True)


def render_komoditi_nonmakanan(non_makanan):
    """Blok IV.2 - Pengeluaran Non Makanan."""
    if not non_makanan:
        st.info("Tidak ada data pengeluaran non makanan.")
        return

    rows = []
    for item in non_makanan:
        rows.append({
            "No Urut": item.get("NoUrutKomoditiFK", "-"),
            "Kode COICOP": item.get("KodeCOICOP") or "-",
            "Komoditi": item.get("Rincian") or "-",
            "K5 Qty": fmt_number(item.get("Kolom5")),
            "K6 Nilai (Rp)": fmt_currency(item.get("Kolom6")),
            "Lainnya": lainnya_str(item),
        })
    st.dataframe(pd.DataFrame(rows), hide_index=True)


def render_pendapatan_5a(pendapatan_5a, rekap):
    """Blok V.A - Pendapatan Pekerjaan Utama + row JUMLAH."""
    rows = []
    for idx, item in enumerate(pendapatan_5a, 1):
        rows.append({
            "No": idx,
            "Jenis Pekerjaan": item.get("MB5AK2") or "-",
            "Lap. Usaha": val_or_dash(item.get("MB5AK3")),
            "Bulan Kerja": val_or_dash(item.get("MB5AK4")),
            "Gaji/Upah (Rp)": fmt_currency(item.get("MB5AK5")),
            "THR/Bonus (Rp)": fmt_currency(item.get("MB5AK6")),
            "Lainnya (Rp)": fmt_currency(item.get("MB5AK7")),
        })

    # Tambahkan row JUMLAH dari rekap
    has_jumlah = any(rekap.get(k) is not None for k in ("B5AK5J", "B5AK6J", "B5AK7J"))
    if has_jumlah:
        rows.append({
            "No": "JUMLAH",
            "Jenis Pekerjaan": "",
            "Lap. Usaha": "",
            "Bulan Kerja": "",
            "Gaji/Upah (Rp)": fmt_currency(rekap.get("B5AK5J")),
            "THR/Bonus (Rp)": fmt_currency(rekap.get("B5AK6J")),
            "Lainnya (Rp)": fmt_currency(rekap.get("B5AK7J")),
        })

    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True)
    else:
        st.info("Tidak ada data pendapatan pekerjaan utama.")


def render_pendapatan_5b(pendapatan_5b, rekap):
    """Blok V.B - Pendapatan Usaha Sendiri + row JUMLAH."""
    rows = []
    for idx, item in enumerate(pendapatan_5b, 1):
        rows.append({
            "No": idx,
            "Jenis Usaha": item.get("MB5BK2") or "-",
            "Lap. Usaha": val_or_dash(item.get("MB5BK3")),
            "Bulan Usaha": val_or_dash(item.get("MB5BK4")),
            "Pend. Kotor (Rp)": fmt_currency(item.get("MB5BK5")),
            "Biaya Produksi (Rp)": fmt_currency(item.get("MB5BK6")),
            "Pend. Bersih (Rp)": fmt_currency(item.get("MB5BK7")),
        })

    has_jumlah = any(rekap.get(k) is not None for k in ("B5BK5J", "B5BK6J", "B5BK7J"))
    if has_jumlah:
        rows.append({
            "No": "JUMLAH",
            "Jenis Usaha": "",
            "Lap. Usaha": "",
            "Bulan Usaha": "",
            "Pend. Kotor (Rp)": fmt_currency(rekap.get("B5BK5J")),
            "Biaya Produksi (Rp)": fmt_currency(rekap.get("B5BK6J")),
            "Pend. Bersih (Rp)": fmt_currency(rekap.get("B5BK7J")),
        })

    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True)
    else:
        st.info("Tidak ada data pendapatan usaha sendiri.")


def render_blok_rekap_table(blok_name, rows_dict):
    """Render satu blok rekap sebagai tabel DataFrame.

    rows_dict: { row_num_str: { 'K2': val, 'K3': val, … } }
    """
    sorted_row_nums = sort_rows(blok_name, list(rows_dict.keys()))

    # Collect all columns across all rows, sorted
    all_cols = set()
    for rd in rows_dict.values():
        all_cols.update(rd.keys())
    columns = sorted(all_cols, key=lambda c: int(re.search(r"\d+", c).group()))

    total_rows_set = TOTAL_ROWS.get(blok_name, set())

    data = []
    for rnum in sorted_row_nums:
        label = row_label(blok_name, rnum)
        is_total = rnum in total_rows_set
        if is_total:
            label = f"**{label} (JUMLAH)**"
        row_out = {"Row": label}
        for col in columns:
            val = rows_dict[rnum].get(col)
            row_out[col] = fmt_currency(val)
        data.append(row_out)

    if data:
        st.dataframe(pd.DataFrame(data), hide_index=True)


# ---------------------------------------------------------------------------
# Main Streamlit App
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Kuesioner Susenas", layout="wide")
st.title("Kuesioner Susenas")

# ---------------------------------------------------------------------------
# Filter dropdowns (cascade)
# ---------------------------------------------------------------------------
conn = get_db_connection()
cur = conn.cursor()

cur.execute("SELECT DISTINCT Kab, Prop FROM rekap WHERE Kab IS NOT NULL ORDER BY Kab")
kab_rows = cur.fetchall()
kab_options = {f"{r['Kab']} ({r['Prop']})": r["Kab"] for r in kab_rows}

col_kab, col_nks, col_nurt = st.columns(3)

selected_kab_display = col_kab.selectbox("Kabupaten:", options=list(kab_options.keys()))
selected_kab = kab_options.get(selected_kab_display)

nks_list = []
if selected_kab:
    cur.execute("SELECT DISTINCT NKS FROM rekap WHERE Kab = ? ORDER BY NKS", (selected_kab,))
    nks_list = [r["NKS"] for r in cur.fetchall()]
selected_nks = col_nks.selectbox("NKS:", options=nks_list, disabled=not nks_list)

nurt_list = []
if selected_kab and selected_nks:
    cur.execute("SELECT DISTINCT Nurt FROM rekap WHERE Kab = ? AND NKS = ? ORDER BY Nurt",
                (selected_kab, selected_nks))
    nurt_list = [r["Nurt"] for r in cur.fetchall()]
selected_nurt = col_nurt.selectbox("No. Urut:", options=nurt_list, disabled=not nurt_list)

conn.close()

# ---------------------------------------------------------------------------
# Auto-render data begitu No. Urut tersedia
# ---------------------------------------------------------------------------
if not (selected_kab and selected_nks and selected_nurt):
    st.info("Silakan lengkapi pilihan di atas untuk menampilkan data.")
    st.stop()

rekap = load_rekap(selected_kab, selected_nks, selected_nurt)
if not rekap:
    st.error("Data tidak ditemukan.")
    st.stop()

komoditi = load_komoditi(selected_kab, selected_nks, selected_nurt)
pendapatan_5a = load_pendapatan_5a(selected_kab, selected_nks, selected_nurt)
pendapatan_5b = load_pendapatan_5b(selected_kab, selected_nks, selected_nurt)

st.divider()

# Identitas Rumah Tangga
st.subheader("Identitas Rumah Tangga")
render_identitas(rekap)

st.divider()

# Blok IV.1 – Pengeluaran Makanan
makanan, individu, non_makanan = split_komoditi(komoditi)

st.subheader("Blok IV.1 - Pengeluaran Makanan (Seminggu)")
render_komoditi_makanan(makanan)

st.subheader("Blok IV.1 - Pengeluaran Makanan Individu (Seminggu)")
render_komoditi_individu(individu)

st.subheader("Blok IV.2 - Pengeluaran Non Makanan (Sebulan/Setahun)")
render_komoditi_nonmakanan(non_makanan)

# Blok IV – Pengeluaran Rekapitulasi
blok_tables = parse_blok_data(rekap)
blok4_keys = [k for k in blok_tables if k.startswith("B43")]
if blok4_keys:
    st.subheader("Blok IV - Pengeluaran (Rekapitulasi)")
    for bk in sorted(blok4_keys):
        st.markdown(f"**{BLOK_TITLES.get(bk, bk)}**")
        render_blok_rekap_table(bk, blok_tables[bk])

# Blok V.A & V.B
st.subheader("Blok V.A - Pendapatan Pekerjaan Utama")
render_pendapatan_5a(pendapatan_5a, rekap)

st.subheader("Blok V.B - Pendapatan Usaha Sendiri")
render_pendapatan_5b(pendapatan_5b, rekap)

# Blok V.C–G
blok5_keys = [k for k in blok_tables if re.match(r"^B5[C-G]$", k)]
if blok5_keys:
    st.subheader("Blok V.C-G - Pendapatan dan Penerimaan (Rekapitulasi)")
    for bk in sorted(blok5_keys):
        st.markdown(f"**{BLOK_TITLES.get(bk, bk)}**")
        render_blok_rekap_table(bk, blok_tables[bk])

# Blok VI dan VII
blok67_keys = [k for k in blok_tables if k in ("B6", "B7")]
if blok67_keys:
    st.subheader("Blok VI dan VII - Pengeluaran dan Pendapatan")
    for bk in sorted(blok67_keys):
        st.markdown(f"**{BLOK_TITLES.get(bk, bk)}**")
        render_blok_rekap_table(bk, blok_tables[bk])
