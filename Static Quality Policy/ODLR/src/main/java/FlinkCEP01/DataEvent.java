package FlinkCEP01;

import java.util.ArrayList;
import java.util.Objects;

public class DataEvent {
    private ArrayList<AttributeValue> data;
    // timestamp is embedded in kafka record;

    public DataEvent(ArrayList<AttributeValue> av){
        this.data = av;
    }
    public DataEvent(){

    }
    public DataEvent(String s){

        this.data = ExtractDataEvent(s).getData();

        //System.out.println("in constructor : "+ DataEventToString(event) );

    }

    public ArrayList<AttributeValue> getData(){return this.data;}


    public void setData(ArrayList<AttributeValue> data) {
        this.data = data;
    }

    public void addData(ArrayList<AttributeValue> data, AttributeValue av){
        data.add(av);
    }

    public String DataEventToString(DataEvent dataEvent){
        String attvalue = "";
        for (int i=0; i<dataEvent.data.size(); i++){
            attvalue += data.get(i).getAttributeName() + " " + data.get(i).getAttributeValue() + " ";
        }
        return attvalue;
    }

    public String getTimeStamp(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("TimeStamp")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }

    public String getProducerID(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("ProducerID")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }

    public String getType(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("Type")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }

    public String getValue(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("Value")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }
    /*
    public String getGroundTruth(){

        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("GroundTruth")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }


    public static String getUncertainty(DataEvent dataEvent){

        for (AttributeValue av:dataEvent.getData()) {
            if (av.getAttributeName().equals("Uncertainty")){
                return av.getAttributeValue();
            }
        }
        return "Not Found";
    }

    public static String CalculateUncertainty(String GT, String V){
        double gt=Double.parseDouble(GT);
        double v = Double.parseDouble(V);
        return String.valueOf(Math.abs(gt-v)/gt);
    }


    public static String getEventDataTimeStamp(DataEvent dataEvent){

        for (AttributeValue av:dataEvent.getData()) {
            if (av.getAttributeName().equals("DataTimeStamp")){
                return av.getAttributeValue();
            }
        }
        return "Not Found";
    }

     */

    public static DataEvent ExtractDataEvent(String string) {
        String[] args = string.split("\\s+");

        ArrayList<AttributeValue> al=new ArrayList<AttributeValue>();

        int i =0;
        AttributeValue av=null;
        for (String s1:args
        ) {
            //System.out.println(s1 + " ");

            if (i==1){
                av.setAttributeValue(s1);
                al.add(av);
                //System.out.println(s1 + "  if ");
                i--;
            }else {
                av= new AttributeValue();
                av.setAttributeName(s1);
                //System.out.println(s1 + "  else");
                i++;
            }

        }
        //System.out.println(al.toString());
        DataEvent dataEvent=new DataEvent(al);
        //System.out.println(dataEvent.getData().get(2).getAttributeName() + dataEvent.getData().get(2).getAttributeValue());
        //System.out.println("in extract method "+DataEventToString(dataEvent));

        return dataEvent;
    }

    /*
    public String CumulativeUncertainty(ArrayList<DataEvent> deList){
        ArrayList<Double> uncertaintyValues = new ArrayList<>();
        for (DataEvent de:deList) {
            System.out.println("event in cumulative : "+de);

            uncertaintyValues.add(1-(Math.abs(Double.parseDouble(de.getValue())-Double.parseDouble(de.getGroundTruth()))/de.extractProducer(de.getProducerID()).getRange()));
        }

        double result = 0;
        for (double d:uncertaintyValues) {
            result += d;
        }
        return String.valueOf(result/uncertaintyValues.size());
    }

     */

    public String getSendingLatency(){
        for (AttributeValue av:this.getData()) {
            if (av.getAttributeName().equals("SendingLatency")){
                return av.getAttributeValue();

            }
        }
        return "Not Found";
    }

    public Producer extractProducer(String pID){
        for (Producer p:Controller.producerList) {
            if (p.getID().equals(pID)){
                return p;
            }
        }
        return null;
    }

    @Override
    public String toString(){
        String attvalue = "";
        for (int i=0; i<this.data.size(); i++){
            attvalue += data.get(i).getAttributeName() + " " + data.get(i).getAttributeValue() + " ";
        }
        return attvalue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataEvent) {
            DataEvent other = (DataEvent) obj;

            return data.containsAll(other.data);
        }
        return false;
    }

}
