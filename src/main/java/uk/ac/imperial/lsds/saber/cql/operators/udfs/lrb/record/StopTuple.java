package uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record;

public class StopTuple {

    public int pos, count;

    public StopTuple(int p, int c) {
        pos = p;
        count = c;
    }
    @Override
    public String toString() {
        return "this.pos: " + this.pos + ", this.count: " + this.count;
    }
}
