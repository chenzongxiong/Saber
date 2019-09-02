package uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record;

public class AvgSpeed {

    public int speed;
    public int count;

    public AvgSpeed(int speed, int cnt) {
        this.speed = speed;
        this.count = cnt;
    }
    @Override
    public String toString() {
        return "this.speed: " + this.speed + ", this.count: " + this.count;
    }
}
