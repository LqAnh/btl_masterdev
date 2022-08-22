# btl_masterdev
Bài tập lớn: Xây dựng `luồng dữ liệu` và các `dashboard` giám sát lượng điện tiêu thụ của hộ gia đình

Dữ liệu bao gồm lượng điện tiêu thụ các thiết bị điện của 6 hộ gia đình trong khoảng từ năm 2015 – 2019.
https://data.open-power-system-data.org/household_data/2020-04-15

-	Mô hình và các component sử dụng được mô tả ở hình vẽ bên dưới:

1)	Dữ liệu được đọc từ file và gửi gitvào kafka topic
2)	Spark Strutured Streaming consume dữ liệu
3)	Dữ liệu được xử lý và lưu vào HDFS
4)	Hive load dữ liệu từ HDFS lên tạo bảng Hive
5)	Sử dụng superset kết nối với Hive để tạo các dashboard trực quan hoá dữ liệu

<img width="953" alt="Screen Shot 2022-08-22 at 18 09 13" src="https://user-images.githubusercontent.com/72133178/185907481-78f595f2-79fb-4667-9f85-0a27884636e2.png">


Kafka được chạy trên server `172.17.80.28` topic `anhlq36_electric` 

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
