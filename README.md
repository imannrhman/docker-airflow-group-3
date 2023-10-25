# Docker Airflow - Kelompok 3

Anggota :
- Iman Nurohman
- Nurul Azzahro
- Shindy Ainun Nabilla
- Yofan Wellyhans


## Cara Menjalankan

Buka terminal dan tuliskan command berikut

```bash
docker compose up
```

Buka terminal lain dan lakukan perubahan permission pada file `airflow.sh` dengan menuliskan command berikut 

```bash
chmod +x airflow.sh
```

Cek apakah file `airflow.sh` dapat dijalankan

```bash
./airflow.sh info
```

Jika berhasil dijalankan lalu export variables pada folder `/exports`

```bash
./airflow.sh variables import exports/variables.json
```

(Optional) Kita juga bisa mengexport untuk connections jika tidak ingin custom connection database 

```bash
./airflow.sh connections import exports/connections.json
```

Buka `localhost:8080` untuk melakukan login  

```bash
open http://localhost:8080/
```

Username dan Password Default

```dosini
USERNAME="airflow"
PASSWORD="airflow"
```
