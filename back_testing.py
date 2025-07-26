import pyodbc
import pandas as pd
from collections import Counter
from tqdm import tqdm # Library untuk progress bar, install dengan: pip install tqdm

# --- Konfigurasi Koneksi SQL Server Anda ---
SERVER_NAME = 'MS-DL7390-DSFB7\SQLEXPRESS'
DATABASE_NAME = 'GamesMatrix'
TABLE_NAME = 'LogGame'

CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)

# --- Variabel Global ---
NUM_RESULTS_TO_OUTPUT = 8800 # Target jumlah hasil akhir
BACKTEST_TARGET_COUNT = 9000 # Jumlah kandidat yang digenerate selama backtesting, sedikit lebih tinggi
NUM_BENCHMARK_PATTERNS = 15  # Berapa banyak pola teratas yang akan dijadikan benchmark

# --- Fungsi Bantuan & Analisis ---
def get_log_game_data(server, database, table, conn_str):
    try:
        print(f"Mencoba mengambil data dari tabel '{table}'...")
        cnxn = pyodbc.connect(conn_str)
        query = f"SELECT Periode, LogResult FROM {table} WHERE GameCode='MQ22' ORDER BY Periode ASC"
        df = pd.read_sql(query, cnxn)
        df['LogResult_Str'] = df['LogResult'].astype(str).str.zfill(4)
        print(f"Data berhasil diambil! Total {len(df)} periode.")
        return df
    except pyodbc.Error as ex:
        print(f"Terjadi kesalahan saat mengambil data: {ex.args[1]}")
        return None

def extract_pattern_type(source_string):
    """Mengubah sumber detail menjadi tipe pola umum untuk benchmarking."""
    if not isinstance(source_string, str): return "Sumber Tidak Diketahui"
    
    parts = source_string.split()
    if source_string.startswith("ekspansi level"):
        return f"Ekspansi Level {parts[2]}"
    if source_string.startswith("dari analisa"):
        return f"Analisa {parts[2]} Digit {parts[3]}"
    if source_string.startswith("dari Analisis Kenaikan/Penurunan"):
        return "Analisis Kenaikan/Penurunan"
    if source_string.startswith("dari Analisis Lompatan Nilai"):
        return "Analisis Lompatan Nilai"
    return source_string

def add_near_log_candidates(predictions_dict, df, last_log, search_type, num_digits, window=1):
    source_prefix = f"dari analisa {num_digits} Digit"
    pattern = ""
    if search_type == 'depan':
        pattern = last_log[:num_digits]
        matching_rows = df[df['LogResult_Str'].str.startswith(pattern)]
        source_prefix += f" Depan {pattern}"
    elif search_type == 'tengah':
        pattern = last_log[1:1+num_digits]
        matching_rows = df[df['LogResult_Str'].str[1:1+num_digits] == pattern]
        source_prefix += f" Tengah {pattern}"
    elif search_type == 'belakang':
        pattern = last_log[4-num_digits:]
        matching_rows = df[df['LogResult_Str'].str.endswith(pattern)]
        source_prefix += f" Belakang {pattern}"
    else: return

    for _, row in matching_rows.iterrows():
        start_idx = max(0, row.name - window)
        end_idx = min(len(df) - 1, row.name + window)
        for i in range(start_idx, end_idx + 1):
            log_result = df.iloc[i]['LogResult_Str']
            if log_result not in predictions_dict:
                predictions_dict[log_result] = source_prefix

def expand_candidates_iteratively(base_dict, target_count):
    final_dict = base_dict.copy()
    source_keys = list(base_dict.keys())
    level = 1
    while len(final_dict) < target_count:
        newly_gen = {}
        mod_combs = list(set((i, j) for i in range(1, level + 1) for j in range(1, level + 1)))
        
        for num_str in source_keys:
            if len(final_dict) + len(newly_gen) >= target_count: break
            num_list = [int(d) for d in num_str]
            for d_idx in range(4):
                for mod_up, mod_down in mod_combs:
                    # Naik
                    new_d_up = num_list[d_idx] + mod_up
                    if 0 <= new_d_up <= 9:
                        new_l = list(num_list); new_l[d_idx] = new_d_up
                        new_n = "".join(map(str, new_l))
                        if new_n not in final_dict and new_n not in newly_gen:
                            newly_gen[new_n] = f"ekspansi level {level} dari {num_str}"
                    # Turun
                    new_d_down = num_list[d_idx] - mod_down
                    if 0 <= new_d_down <= 9:
                        new_l = list(num_list); new_l[d_idx] = new_d_down
                        new_n = "".join(map(str, new_l))
                        if new_n not in final_dict and new_n not in newly_gen:
                            newly_gen[new_n] = f"ekspansi level {level} dari {num_str}"
        
        if not newly_gen: break
        final_dict.update(newly_gen)
        level += 1
    return final_dict

def generate_full_prediction_set(historical_df, last_log_result_str, target_count):
    """Fungsi utama untuk menghasilkan satu set prediksi lengkap untuk satu periode."""
    predictions = {}
    
    # NearLog Analysis
    for nd in [2, 3]:
        for st in ['depan', 'tengah', 'belakang']:
            add_near_log_candidates(predictions, historical_df, last_log_result_str, st, nd)

    # Anda bisa menambahkan fungsi analisis lompatan/penurunan di sini jika diinginkan
    # ...

    # Expansion
    if len(predictions) < target_count:
        predictions = expand_candidates_iteratively(predictions, target_count)
        
    return predictions

# --- Program Utama ---
if __name__ == "__main__":
    df_log = get_log_game_data(SERVER_NAME, DATABASE_NAME, TABLE_NAME, CONNECTION_STRING)

    if df_log is not None and len(df_log) > 1:
        
        # =================================================================
        # FASE 1: BACKTESTING & PENEMUAN POLA BENCHMARK
        # =================================================================
        print("\n--- FASE 1: Memulai Backtesting untuk Menemukan Pola Benchmark ---")
        pattern_benchmark_counter = Counter()
        
        # Kita loop dari periode kedua hingga sebelum periode terakhir
        for i in tqdm(range(1, len(df_log) - 1), desc="Backtesting"):
            # Siapkan data untuk iterasi ini
            previous_period_log = df_log.iloc[i-1]['LogResult_Str']
            current_period_actual_log = df_log.iloc[i]['LogResult_Str']
            historical_data_for_test = df_log.iloc[:i]
            
            # Hasilkan prediksi seolah-olah kita berada di periode sebelumnya
            predictions_for_test = generate_full_prediction_set(
                historical_data_for_test, 
                previous_period_log, 
                BACKTEST_TARGET_COUNT
            )
            
            # Periksa apakah hasil aktual ada di dalam prediksi kita
            if current_period_actual_log in predictions_for_test:
                winning_source = predictions_for_test[current_period_actual_log]
                pattern_type = extract_pattern_type(winning_source)
                pattern_benchmark_counter[pattern_type] += 1
        
        print("\n--- Hasil Backtesting ---")
        if not pattern_benchmark_counter:
            print("Tidak ada pola benchmark yang ditemukan selama backtesting.")
            benchmark_patterns = []
        else:
            print(f"Ditemukan {len(pattern_benchmark_counter)} tipe pola yang berhasil.")
            print(f"{NUM_BENCHMARK_PATTERNS} Pola Benchmark Teratas:")
            benchmark_patterns = []
            for i, (pattern, count) in enumerate(pattern_benchmark_counter.most_common(NUM_BENCHMARK_PATTERNS)):
                print(f"{i+1}. '{pattern}' (berhasil {count} kali)")
                benchmark_patterns.append(pattern)

        # =================================================================
        # FASE 2: PREDIKSI FINAL MENGGUNAKAN BENCHMARK
        # =================================================================
        print("\n--- FASE 2: Membuat Prediksi Final dengan Prioritas Benchmark ---")
        
        final_last_log = df_log.iloc[-1]['LogResult_Str']
        print(f"LogResult terakhir untuk prediksi: {final_last_log} (dari Periode {df_log.iloc[-1]['Periode']})")

        # Hasilkan satu set prediksi lengkap menggunakan semua data historis
        final_predictions_dict = generate_full_prediction_set(
            df_log, 
            final_last_log, 
            NUM_RESULTS_TO_OUTPUT
        )
        
        # Urutkan ulang hasil prediksi berdasarkan benchmark
        prioritized_results = []
        other_results = []

        for number, source in final_predictions_dict.items():
            pattern_type = extract_pattern_type(source)
            if pattern_type in benchmark_patterns:
                prioritized_results.append((number, source))
            else:
                other_results.append((number, source))
        
        # Gabungkan daftar, dengan hasil prioritas di paling atas
        final_sorted_output = prioritized_results + other_results
        
        # Potong sesuai jumlah output yang diinginkan
        final_output_list = final_sorted_output[:NUM_RESULTS_TO_OUTPUT]

        # =================================================================
        # FASE 3: MENYIMPAN HASIL
        # =================================================================
        print(f"\n--- FASE 3: Menyimpan Hasil Akhir ---")
        print(f"Total {len(final_output_list)} nomor prediksi telah dihasilkan dan diurutkan.")
        
        if final_output_list:
            output_filename = f'predicted_numbers_benchmarked_MQ22_{len(final_output_list)}.txt'
            with open(output_filename, 'w') as f:
                f.write("--- HASIL DENGAN PRIORITAS BENCHMARK ---\n")
                for number, source in prioritized_results:
                     if (number, source) in final_output_list:
                        f.write(f"{number} --> {source} [BENCHMARK]\n")
                
                f.write("\n--- HASIL LAINNYA ---\n")
                for number, source in other_results:
                    if (number, source) in final_output_list:
                        f.write(f"{number} --> {source}\n")

            print(f"Semua {len(final_output_list)} nomor telah disimpan ke '{output_filename}'")
            print("\nContoh Hasil Teratas:")
            for i, (number, source) in enumerate(final_output_list[:15]):
                 is_benchmark = extract_pattern_type(source) in benchmark_patterns
                 print(f"{i+1}. {number} --> {source} {'[BENCHMARK]' if is_benchmark else ''}")

        else:
            print("Tidak ada nomor yang berhasil diprediksi/dihasilkan.")

        print("\nProses selesai!")

    else:
        print("Gagal mendapatkan data atau data historis tidak cukup untuk memulai proses.")