package FlinkCEP01;



import java.io.Serializable;

public class ProducerEventTypeMatch implements Serializable {
    Producer p;
    SE et;
    Query q;
    int Nqp=0;
    long matchedTime;

    public Query getQ() {
        return q;
    }

    public void setQ(Query q) {
        this.q = q;
    }

    public long getMatchedTime() {
        return matchedTime;
    }

    public void setMatchedTime(long matchedTime) {
        this.matchedTime = matchedTime;
    }

    public Producer getP() {
        return p;
    }

    public void setP(Producer p) {
        this.p = p;
    }

    public SE getEt() {
        return et;
    }

    public void setEt(SE et) {
        this.et = et;
    }

    public int getNqp() {
        return Nqp;
    }

    public void setNqp(int nqp) {
        Nqp = nqp;
    }

    public ProducerEventTypeMatch(){}

    public ProducerEventTypeMatch(Producer p, SE et, Query q) {
        this.p = p;
        this.et = et;
        this.q = q;
        //this.Nqp = n;
    }
}
