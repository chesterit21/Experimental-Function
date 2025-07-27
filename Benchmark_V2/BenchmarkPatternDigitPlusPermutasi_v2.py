"""
Skrip ini melakukan analisis backtesting pada data game historis dari database.
Tujuannya adalah untuk menghasilkan sekumpulan angka kandidat berdasarkan pola
yang ditemukan dari hasil game terakhir.

Alur kerja utama adalah sebagai berikut:
1. Mengambil data historis untuk setiap game.
2. Mencari pola dari hasil terakhir di data historis untuk mendapatkan "temuan awal".
3. Melakukan permutasi pada temuan awal untuk memperbanyak kandidat.
4. Melakukan ekspansi kombinatorial pada kandidat yang ada hingga mencapai target jumlah.
5. Menyimpan hasil akhir ke dalam file teks untuk analisis lebih lanjut.
"""
import pyodbc
import pandas as pd
import os
from tqdm import tqdm
import itertools

# --- Konfigurasi Utama ---
SERVER_NAME = 'MS-DL7390-DSFB7\SQLEXPRESS'
DATABASE_NAME = 'GamesMatrix'
CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)

# --- Konfigurasi Direktori dan Proses ---
OUTPUT_DIR = "Data_Analisa"
TARGET_CANDIDATE_COUNT = 9450 
 
# --- FUNGSI-FUNGSI UTILITAS & PEMROSESAN ---
def generate_permutations(number_str):
    """
    Langkah 3.1: Menghasilkan semua permutasi unik dari sebuah string angka.
    Contoh: '123' -> {'123', '132', '213', '231', '312', '321'}

    Args:
        number_str (str): String angka 4 digit yang akan dipermutasi.

    Returns:
        set: Sebuah set berisi semua hasil permutasi yang unik.
    """
    perms = set(itertools.permutations(number_str))
    return {"".join(p) for p in perms}

def expand_candidates_iteratively(base_dict, target_count):
    """
    Langkah 4: Melakukan ekspansi kombinatorial pada kandidat angka yang ada.
    
    Proses ini berjalan secara iteratif per level. Di setiap level, setiap digit
    dari angka sumber akan dimodifikasi dengan menambah atau mengurangi nilai
    sebesar `level`. Kombinasi baru yang valid (0-9) dan belum ada akan
    ditambahkan ke dalam daftar kandidat hingga `target_count` tercapai.

    Args:
        base_dict (dict): Dictionary awal berisi kandidat angka (key) dan 
                          sumbernya (value).
        target_count (int): Jumlah total kandidat yang ingin dicapai.

    Returns:
        dict: Dictionary final yang telah diperluas hingga mendekati target.
    """
    final_dict = base_dict.copy()
    if len(final_dict) >= target_count:
        return final_dict
        
    source_keys = list(base_dict.keys())
    level = 1
    while len(final_dict) < target_count:
        print(f"  Ekspansi Kombinatorial Level {level} dimulai...")
        newly_gen_in_level = {}
        
        for num_str in source_keys:
            if len(final_dict) + len(newly_gen_in_level) >= target_count:
                break
            num_list = [int(d) for d in num_str]
            all_digit_possibilities = []
            for digit_char in num_str:
                original_digit = int(digit_char)
                single_digit_options = {original_digit}
                for mod in range(1, level + 1):
                    single_digit_options.add(original_digit + mod)
                    single_digit_options.add(original_digit - mod)
                
                valid_options = sorted([opt for opt in single_digit_options if 0 <= opt <= 9])
                all_digit_possibilities.append(valid_options)

            for combo_tuple in itertools.product(*all_digit_possibilities):
                new_num = "".join(map(str, combo_tuple))
                if new_num == num_str:
                    continue
                if new_num not in final_dict and new_num not in newly_gen_in_level:
                    mods = [new_digit - old_digit for new_digit, old_digit in zip(combo_tuple, num_list)]
                    mods_str = ",".join(f"{m:+d}" for m in mods)
                    newly_gen_in_level[new_num] = f"ekspansi kombinatorial level {level} dari {num_str} ({mods_str})"
                    if len(final_dict) + len(newly_gen_in_level) >= target_count:
                        break
        if not newly_gen_in_level:
            print("  Ekspansi level ini tidak menghasilkan kandidat baru. Menghentikan proses.")
            break
        final_dict.update(newly_gen_in_level)
        level += 1
    return final_dict

# --- FUNGSI-FUNGSI DATABASE ---

def get_game_codes(cnxn):
    """
    Mengambil semua GameCode unik dari tabel MasterGame di database.

    Args:
        cnxn: Koneksi database pyodbc yang aktif.

    Returns:
        list: Sebuah list berisi string GameCode. Mengembalikan list kosong jika gagal.
    """
    print("Mengambil daftar GameCode dari MasterGame...")
    try:
        query = "SELECT DISTINCT GameCode FROM MasterGame"
        df = pd.read_sql(query, cnxn)
        if df.empty:
            return []
        return df['GameCode'].tolist()
    except Exception as e:
        print(f"Error saat mengambil GameCode: {e}")
        return []

def get_log_data_for_game(cnxn, game_code):
    """
    Mengambil data log historis untuk sebuah GameCode spesifik dari tabel LogGameBenchmark.
    
    Data yang diambil diurutkan berdasarkan periode, dan LogResult diformat menjadi
    string 4 digit dengan zero-padding.

    Args:
        cnxn: Koneksi database pyodbc yang aktif.
        game_code (str): Kode game yang datanya akan diambil.

    Returns:
        pd.DataFrame: DataFrame berisi data log dengan Periode sebagai index, atau None jika gagal/tidak ada data.
    """
    try:
        query = f"SELECT Periode, LogResult FROM LogGameBenchmark WHERE GameCode='{game_code}' ORDER BY Periode ASC"
        df = pd.read_sql(query, cnxn)
        if df.empty:
            return None
        df['LogResult'] = df['LogResult'].astype(str).str.zfill(4)
        df.set_index('Periode', inplace=True)
        return df
    except Exception as e:
        print(f"Error saat mengambil data untuk GameCode '{game_code}': {e}")
        return None

# --- FUNGSI PROSES UTAMA ---

def process_game_data(df, game_code):
    """
    Fungsi inti yang menjalankan seluruh alur analisis untuk satu GameCode.
    
    Langkah-langkah yang dilakukan:
    1. Mengidentifikasi hasil game terakhir.
    2. Mengekstrak pola dari hasil terakhir dan mencarinya di data historis.
    3. Mengumpulkan `LogResult` dari data yang cocok, sebelum, dan sesudahnya sebagai "temuan awal".
    4. Membuat kandidat angka dengan melakukan permutasi pada temuan awal.
    5. Melakukan ekspansi kombinatorial pada semua kandidat hingga mencapai target.
    6. Menyimpan hasil akhir ke dalam dua file: satu file berformat detail dan satu file data murni.

    Args:
        df (pd.DataFrame): DataFrame berisi data log historis untuk satu game.
        game_code (str): Kode game yang sedang diproses.
    """
    if df is None or len(df) < 2:
        return

    latest_data = df.iloc[-1]
    latest_periode = latest_data.name
    latest_result = latest_data['LogResult']
    
    print(f"\nMemproses GameCode: {game_code}")
    print(f"Data Terakhir -> Periode: {latest_periode}, Hasil: {latest_result}")

    patterns = {
        "2D Depan": latest_result[0:2], "2D Tengah": latest_result[1:3],
        "2D Belakang": latest_result[2:4], "3D Depan": latest_result[0:3],
        "3D Belakang": latest_result[1:4], "4D Asli": latest_result
    }
    historical_data = df.iloc[:-1]
    
    # Langkah 1: Mengumpulkan hasil temuan awal (LogResult)
    # Variabel ini menggunakan `set` untuk memastikan semua LogResult yang terkumpul unik.
    initial_found_results = set()
    print("Langkah 1: Mengumpulkan hasil temuan awal (LogResult)...")
    for pattern_name, pattern_value in patterns.items():
        matches = historical_data[historical_data['LogResult'].str.contains(pattern_value, na=False)]
        for matched_periode, matched_row in matches.iterrows():
            loc = df.index.get_loc(matched_periode)
            
            # Ambil LogResult dari data sebelum, data yang cocok, dan data sesudah.
            if loc > 0:
                initial_found_results.add(df.iloc[loc - 1]['LogResult'])
            initial_found_results.add(matched_row['LogResult'])
            if loc < len(df) - 1:
                initial_found_results.add(df.iloc[loc + 1]['LogResult'])
    
    # Jika tidak ada temuan sama sekali, lewati game ini.
    if not initial_found_results:
        print("Tidak ada hasil temuan awal. Melewati GameCode ini.")
        return
    print(f"Ditemukan {len(initial_found_results)} hasil temuan awal (LogResult) yang unik.")

    # Langkah 2 & 3: Membuat kandidat dari temuan awal dan permutasinya.
    # Penggabungan ini memastikan tidak ada duplikasi antara temuan awal dan hasil permutasinya.
    # Penggunaan dictionary comprehension memastikan keunikan secara otomatis.
    base_candidates = {num: "temuan awal" for num in initial_found_results}

    # Langkah 3: Tambahkan permutasi unik dari setiap temuan awal.
    # Iterasi melalui temuan awal untuk menghasilkan dan menambahkan permutasi baru.
    for num in sorted(list(initial_found_results)): # Diurutkan agar sumber permutasi konsisten
        permutations = generate_permutations(num)
        for p in permutations:
            # Hanya tambahkan permutasi jika ia adalah angka yang benar-benar baru
            # (tidak ada di temuan awal atau permutasi dari angka lain).
            if p not in base_candidates:
                base_candidates[p] = f"permutasi dari {num}"
    print(f"Total {len(base_candidates)} kandidat setelah digabung dengan permutasi.")

    # Langkah 4: Menjalankan ekspansi kombinatorial untuk mencapai target jumlah kandidat.
    print(f"Langkah 4: Menjalankan ekspansi kombinatorial (Target: {TARGET_CANDIDATE_COUNT} kandidat)...")
    final_candidates = expand_candidates_iteratively(base_candidates, TARGET_CANDIDATE_COUNT)
    print(f"Ekspansi selesai. Total kandidat final: {len(final_candidates)}")

    # Langkah 5: Menyimpan hasil akhir ke dalam file.
    print("Langkah 5: Menyimpan hasil final...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # File 0 (BARU): Menyimpan gabungan data awal dan hasil permutasi (refactoring acak posisi).
    combined_output_filepath = os.path.join(OUTPUT_DIR, f"{game_code}_{latest_periode}_data_awal_dan_data_refacoring_acak_posisi.txt")
    with open(combined_output_filepath, 'w') as f_combined:
        # Menggunakan base_candidates yang berisi gabungan data awal dan permutasi
        for num in sorted(base_candidates.keys()):
            f_combined.write(f"{num}\n")
            
    
    # File 1: Menyimpan hasil dengan format detail (angka dan sumbernya).
    formatted_output_filepath = os.path.join(OUTPUT_DIR, f"{game_code}_{latest_periode}_data_hasil_temuan_v3.txt")
    with open(formatted_output_filepath, 'w') as f:
        f.write(f"Analisis Final untuk GameCode: {game_code}\n")
        f.write(f"Total Kandidat: {len(final_candidates)}\n")
        f.write("="*50 + "\n")
        for num, origin in sorted(final_candidates.items()):
            f.write(f"{num} (Sumber: {origin})\n")

    # File 2: Menyimpan data murni (hanya angka kandidat) untuk pemrosesan lebih lanjut.
    pure_output_filepath = os.path.join(OUTPUT_DIR, f"{game_code}_{latest_periode}_pure_data_temuan_v3.txt")
    with open(pure_output_filepath, 'w') as pure_file:
        # Mengurutkan 'keys' (LogResult) sebelum ditulis ke file
        for num in sorted(final_candidates.keys()):
            pure_file.write(f"{num}\n")
            
    print(f"Hasil disimpan di:\n- {combined_output_filepath}\n- {formatted_output_filepath}\n- {pure_output_filepath}")

# --- FUNGSI MAIN ---

def main():
    """
    Fungsi utama (entry point) untuk menjalankan seluruh alur program.
    Mengelola koneksi database, iterasi game, dan penanganan error.
    """
    print("Memulai program analisis pola...")
    cnxn = None
    try:
        cnxn = pyodbc.connect(CONNECTION_STRING)
        print("Koneksi ke database berhasil.")
        
        game_codes = get_game_codes(cnxn)
        if not game_codes:
            return

        for code in tqdm(game_codes, desc="Memproses semua GameCode"):
            df_game = get_log_data_for_game(cnxn, code)
            if df_game is not None:
                process_game_data(df_game, code)

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Error koneksi database: {sqlstate}\n{ex}")
    except Exception as e:
        print(f"Terjadi error yang tidak terduga: {e}")
    finally:
        if cnxn:
            cnxn.close()
            print("\nKoneksi ke database ditutup.")
        print("Program selesai.")

if __name__ == '__main__':
    main()
