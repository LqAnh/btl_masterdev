# btl_masterdev
Bài tập lớn: Xây dựng `luồng dữ liệu` và các `dashboard` giám sát lượng điện tiêu thụ của hộ gia đình

Dữ liệu bao gồm lượng điện tiêu thụ các thiết bị điện của 6 hộ gia đình trong khoảng từ năm 2015 – 2019.

-	Mô hình và các component sử dụng được mô tả ở hình vẽ bên dưới:

1)	Dữ liệu được đọc từ file và gửi gitvào kafka topic
2)	Spark Strutured Streaming consume dữ liệu
3)	Dữ liệu được xử lý và lưu vào HDFS
4)	Hive load dữ liệu từ HDFS lên tạo bảng Hive
5)	Sử dụng superset kết nối với Hive để tạo các dashboard trực quan hoá dữ liệu

<img width="528" alt="image" src="https://user-images.githubusercontent.com/72133178/184479184-791b6d29-cf0d-4fb0-bda8-9ce226af16b5.png">

Kafka được chạy trên máy local  

Câu lệnh start Spark Thrift Server trên server `172.17.80.21` tại `queue 1`

    start-thriftserver.sh --master yarn --num-executors 1  --driver-memory 512m --executor-memory 512m  --executor-cores 1 --driver-cores 1 --queue queue1  --hiveconf hive.server2.thrift.port=10015
Câu lệnh kết nối bằng `beeline` với khi yêu cầu `user` `password` ấn `enter` 
    
    beeline
    !connect jdbc:hive2://0.0.0.0:10015/
Để thoát khỏi `beeline` nhập 
    
    !q
Câu lệnh tạo `hive table` từ `hdfs` để truy vấn với `superset`

    create external table test (id INT, res_id INT, time TIMESTAMP, dishwasher FLOAT, ev FLOAT ,freezer FLOAT, grid_export FLOAT , grid_import FLOAT,heat_pump FLOAT ,pv FLOAT ,refrigerator FLOAT , washing_machine FLOAT) STORED AS PARQUET LOCATION 'hdfs://172.17.80.21:9000/user/anhlq36/btl/output';

Trên server `172.17.80.21` (master) user `hadoop` folder `/home/hadoop/anhlq36/btl` câu lệnh spark-submit:

    spark-submit --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --class com.example.btl.sparkss.SparkSS btl-1.0-SNAPSHOT.jar