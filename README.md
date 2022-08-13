# btl_masterdev
Bài tập lớn: Xây dựng luồng dữ liệu và các dashboard giám sát lượng điện tiêu thụ của hộ gia đình

Dữ liệu bao gồm lượng điện tiêu thụ các thiết bị điện của 6 hộ gia đình trong khoảng từ năm 2015 – 2019.

-	Mô hình và các component sử dụng được mô tả ở hình vẽ bên dưới:

1)	Dữ liệu được đọc từ file và gửi gitvào kafka topic
2)	Spark Strutured Streaming consume dữ liệu
3)	Dữ liệu được xử lý và lưu vào HDFS
4)	Hive load dữ liệu từ HDFS lên tạo bảng Hive
5)	Sử dụng superset kết nối với Hive để tạo các dashboard trực quan hoá dữ liệu

<img width="528" alt="image" src="https://user-images.githubusercontent.com/72133178/184479184-791b6d29-cf0d-4fb0-bda8-9ce226af16b5.png">
