package assignment2;

public class Count {
    int id;
    String dateTime;
    int year;
    String month;
    int mDate;
    String day;
    int time;
    int sensorId;
    String sensorName;
    int hourlyCounts;

//    @Override
//    public String toString() {
//        return "Count{" +
//                "id=" + id +
//                ", dateTime='" + dateTime + '\'' +
//                ", year=" + year +
//                ", month='" + month + '\'' +
//                ", mDate=" + mDate +
//                ", day='" + day + '\'' +
//                ", time=" + time +
//                ", sensorId=" + sensorId +
//                ", sensorName='" + sensorName + '\'' +
//                ", hourlyCounts=" + hourlyCounts +
//                '}';
//    }


    @Override
    public String toString() {
        return String.valueOf(id);
    }
}
