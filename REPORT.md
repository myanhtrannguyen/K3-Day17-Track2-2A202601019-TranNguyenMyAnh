# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trần Nguyễn Mỹ Anh  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 16.5s
  run 2/3 … 16.5s
  run 3/3 … 16.3s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt

</details>

Tổng kết: **4 / 4 tiêu chí dữ liệu đạt**. Bài dashboard trong `EXTRA.md`
không thuộc ba nhiệm vụ bắt buộc.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Chạy lại pipeline làm `gold_training_set` tăng lên 38.750 hàng; 12.480 `ticket_id` bị lặp, dù bảng có grain là một hàng cho một ticket. |
| **Nguyên nhân** | Incremental model không có `unique_key` và `incremental_strategy`, nên dbt dùng `INSERT`/append. Ticket có event `c` rồi `u` được ghi thêm thay vì thay thế bản ghi cũ. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: đặt `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. Đồng thời đặt `catchup=False`, `max_active_runs=1` trong DAG để giảm khả năng nhiều lượt chạy chồng nhau; đây không phải root cause. |
| **Bằng chứng** | trước: 38.750 hàng · sau: 12.480 hàng, không còn `ticket_id` lặp; các lượt chạy lặp lại cho cùng kết quả. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 thay vì 9.100 hàng; thiếu 455 cặp `(event_date, customer_id)`, tập trung ở các ngày cũ 03–13/08. |
| **P99 độ trễ đo được** | **2.73 ngày** *(2.7258 ngày, xấp xỉ 2 ngày 17 giờ 25 phút)* |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 để bao phủ ít nhất 99% event đến muộn. |
| **Nguyên nhân** | Điều kiện `event_date > max(event_date)` chỉ chọn ngày mới hơn ngày lớn nhất đã có trong Gold. Event của ngày cũ nhưng đến warehouse muộn bị loại vĩnh viễn, nên aggregate của ngày đó không được tính lại. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: tính lại cửa sổ từ `run_date - 3 ngày` đến hết `run_date`; đặt `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để thay thế aggregate cũ trong mỗi lần tính lại. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng; không còn cặp `(event_date, customer_id)` trùng. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 phản ánh độ trễ cao nhưng phổ biến hơn các ngoại lệ cực đoan, nên cân
> bằng tốt hơn giữa độ đầy đủ dữ liệu và chi phí. Dùng `max` có thể làm window
> bị chi phối bởi một event hiếm. Mỗi ngày lookback thêm làm tăng lượng dữ liệu
> phải quét, group và `MERGE`; chi phí này phát sinh ở mọi lượt chạy sau đó,
> không chỉ khi backfill.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets` có 6.606 hàng có `priority` NULL hoặc ngoài miền 1..4, trong khi `quarantine_tickets` rỗng. |
| **Nguyên nhân** | `try_cast` coi các nhãn chữ hợp lệ là NULL nhưng lại chấp nhận số ngoài miền như `0`, `5`, `-1`. Ngoài ra, nếu lọc lỗi sau khi chọn bản ghi mới nhất, một CDC record lỗi sẽ làm mất cả ticket. Contract chưa được bật và không có test miền giá trị. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | (1) `1`–`4`: giữ nguyên. (2) `urgent`, `high`, `medium`, `low`: schema evolution, map lần lượt thành 1, 2, 3, 4. (3) `P1`, `P2`, `unknown`, `0`, `5`, `-1`, rỗng, NULL: dữ liệu lỗi, trả về NULL và quarantine. |
| **Cách khắc phục** | `normalize_priority.sql` dùng `CASE` để map hai nhóm hợp lệ. `silver_tickets.sql` chuẩn hoá và lọc record NULL **trước** `row_number`. `quarantine_tickets.sql` dùng cùng macro để chọn record bị loại. `schema.yml` bật contract, thêm `not_null` và `accepted_values: [1,2,3,4]`. |
| **Bằng chứng** | trước: 6.606 priority không hợp lệ, quarantine = 0, `dbt test` 9/9 pass · sau: priority sạch, `quarantine_tickets` = 312, `dbt test` 11/11 pass. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Nên giữ Bronze như bản ghi thô, bất biến để bảo toàn payload, dấu vết thời
> gian và bằng chứng điều tra. Nếu từ chối record ngay ở Bronze, đội vận hành
> mất dữ liệu gốc để tái hiện sự cố hoặc khôi phục sau khi bổ sung mapping.
> 
> Không nên để vài record lỗi làm dừng cả DAG: 312 record hỏng không nên chặn
> 12.480 ticket hợp lệ, hơn 130.000 event và 31.200 chunk. Quarantine giữ
> pipeline phục vụ dữ liệu tốt, đồng thời tạo hàng đợi và tín hiệu để xử lý lỗi.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A — Query dashboard chậm; B — Consumer gặp sự cố giữa batch |
| **Triệu chứng** | Dashboard lọc một khách hàng (`customer_name = 'ACME'`) và một ngày nhưng quét 5.000.000 rows từ 5.000 file Parquet nhỏ. |
| **Nguyên nhân** | File nguồn có tên ngẫu nhiên, không partition theo ngày, nên engine phải mở toàn bộ file mới biết file nào cần. Điều kiện `strftime(event_time, ...)` bọc cột trong function, ngăn partition pruning và predicate pushdown hiệu quả. |
| **Cách khắc phục** | `tools/compact.py` ghi lại dataset theo Hive partition `event_date`, sắp xếp `customer_name, event_time` và dùng row group 2.048 hàng. `queries/dashboard.sql` đọc `gold_events_v2` với `hive_partitioning = true` và lọc trực tiếp `event_date`. |
| **Bằng chứng** | rows scanned: 5.000.000 → 9.324 (**536,3×**) · files: 5.000 → 14 · rows on disk: 130.683 → 130.683 · result hash: `4379e4c5d9f3` không đổi. |

### Bài B — Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | Consumer commit offset trước khi ghi batch. Khi bị giết ở batch 7, restart chỉ có 19.500/20.000 event: mất đúng 500 record của batch đã commit nhưng chưa ghi. |
| **Nguyên nhân** | Thứ tự `commit → crash → write` tạo ngữ nghĩa at-most-once: offset đi trước dữ liệu. Đảo thứ tự mà vẫn `INSERT` thuần sẽ tạo bản ghi trùng khi batch được replay. |
| **Cách khắc phục** | `ingest/consumer.py`: đặt `event_id` làm primary key, ghi bằng `INSERT ... ON CONFLICT (event_id) DO UPDATE`, rồi thực hiện `write → crash point → commit`. Consumer trở thành at-least-once kết hợp ghi idempotent. |
| **Bằng chứng** | Sau crash và restart: 20.000/20.000 event_id khác nhau; không mất, không trùng, `C == A`; `make crash-test` đạt. |

`DO NOTHING` giữ lại payload cũ khi message bị phát lại; nếu payload của
message đã thay đổi thì kho vẫn chứa dữ liệu cũ. `DO UPDATE` thay payload cũ
bằng payload replay, nên được chọn để kết quả cuối cùng phản ánh phiên bản mới
nhất của cùng một `event_id`.

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Xác định grain, natural key và SQL materialization thực tế (`INSERT`, `MERGE` hay `DELETE+INSERT`); sau đó chạy lại cùng input để kiểm tra idempotence. |
| 2 | So sánh `event_time` với thời điểm dữ liệu đến kho, đo percentile độ trễ và đối chiếu lookback window với grain/key của aggregate. |
| 3 | Phân biệt schema evolution với dữ liệu hỏng; giữ Bronze thô, chuẩn hoá ở Silver, quarantine record lỗi và kiểm tra cả contract kiểu dữ liệu lẫn test miền giá trị. |
