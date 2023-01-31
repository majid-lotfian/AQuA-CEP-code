package FlinkCEP01;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;


public class Query implements Serializable {
    private String ID;
    private String consumerID;
    private ArrayList<QualityPolicy> qualityPolicy;
    private ArrayList<SE> etList;
    long startTime;
    private int issueTime;
    private int duration = 3600;
    private boolean active= false;
    private int patternDuration=0;
    private boolean firstAssignment=true;



    public boolean isFirstAssignment() {
        return firstAssignment;
    }
    private String previousProducerID="";

    public String getPreviousProducerID() {
        return previousProducerID;
    }

    public void setPreviousProducerID(String previousProducerID) {
        this.previousProducerID = previousProducerID;
    }
    private boolean averaging = false;

    public boolean isAveraging() {
        return averaging;
    }

    public void setAveraging(boolean averaging) {
        this.averaging = averaging;
    }
    private int[] windowEventCounts = new int[Controller.maxNumOfWindows];

    private ProducerEventTypeMatch currentPET;

    public ProducerEventTypeMatch getCurrentPET() {
        return currentPET;
    }

    public void setCurrentPET(ProducerEventTypeMatch currentPET) {
        this.currentPET = currentPET;
    }

    private int windowSize=50;

    public int getWindowSize() {
        return this.windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public int[] getWindowEventCounts() {
        return windowEventCounts;
    }
    public int getWindowEventCountsByIndex(int index) {
        return windowEventCounts[index];
    }

    public void setWindowEventCounts(int index, int value) {
        this.windowEventCounts[index] = value;
    }
    private double currentProducerLossRate;

    public double getCurrentProducerLossRate() {
        return currentProducerLossRate;
    }

    public void setCurrentProducerLossRate(double currentProducerLossRate) {
        this.currentProducerLossRate = currentProducerLossRate;
    }

    private int windowIndex = 1;

    public int getWindowIndex() {
        return windowIndex;
    }

    public String[][] GlobalAssignedDS=new String[120][2];

    public String[][] getGlobalAssignedDS() {
        return this.GlobalAssignedDS;
    }
    public String getGlobalAssignedDSByIndex(int index1, int index2) {
        return this.GlobalAssignedDS[index1][index2];
    }

    public void setGlobalAssignedDS(int index1, int index2, String value) {
        this.GlobalAssignedDS[index1][index2] = value;
    }

    private int globalNumberOfLostEvents=0;
    private int producerLostEvents=0;

    public int getGlobalNumberOfLostEvents() {
        return globalNumberOfLostEvents;
    }

    public void setGlobalNumberOfLostEvents(int globalNumberOfLostEvents) {
        this.globalNumberOfLostEvents = globalNumberOfLostEvents;
    }

    public int getProducerLostEvents() {
        return producerLostEvents;
    }

    public void setProducerLostEvents(int producerLostEvents) {
        this.producerLostEvents = producerLostEvents;
    }

    public void setWindowIndex(int windowIndex) {
        this.windowIndex = windowIndex;
    }

    public void setFirstAssignment(boolean firstAssignment) {
        this.firstAssignment = firstAssignment;
    }

    private int queryOffset;
    private int producerOffset;

    public int getQueryOffset() {
        return queryOffset;
    }

    public void setQueryOffset(int queryOffset) {
        this.queryOffset = queryOffset;
    }

    public int getProducerOffset() {
        return producerOffset;
    }

    public void setProducerOffset(int producerOffset) {
        this.producerOffset = producerOffset;
    }

    //double wAccuracy;
    double wReuseFactor;
    double wEnergyConsumption;
    public double getwEnergyConsumption() {
        return wEnergyConsumption;
    }

    public void setwEnergyConsumption(double wEnergyConsumption) {
        this.wEnergyConsumption = wEnergyConsumption;
    }
    public double getwReuseFactor() {
        return wReuseFactor;
    }

    public void setwReuseFactor(double wEnergyConsumption) {
        this.wReuseFactor = wEnergyConsumption;
    }



    public int getPatternDuration() {
        return patternDuration;
    }

    public void setPatternDuration(int patternDuration) {
        this.patternDuration = patternDuration;
    }

    public int getIssueTime() {
        return issueTime;
    }

    public void setIssueTime(int issueTime) {
        this.issueTime = issueTime;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public String getID() {
        return ID;
    }

    public String getConsumerID() {
        return consumerID;
    }

    public ArrayList<QualityPolicy> getQualityPolicy() {
        return qualityPolicy;
    }

    public ArrayList<SE> getEtList() {
        return etList;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public void setConsumerID(String issuingUserID) {
        this.consumerID = issuingUserID;
    }

    public void setQualityPolicy(ArrayList<QualityPolicy> qualityPolicy) {
        this.qualityPolicy = qualityPolicy;
    }

    public void setEtList(ArrayList<SE> etList) {
        this.etList = etList;
    }

    public static String setQueryTopic(String s) {
        return s + "Topic";
    }

    public void terminateQuery() throws IOException {
        System.out.println("query "+this.getID()+" is finished !");
        //System.out.println("result size : "+this.getGlobalWindowCounts().length);
        ExportResults(this.getGlobalAssignedDS());
        //Controller.queryList.remove(q);
        for (ProducerEventTypeMatch pet : Controller.globalMatchedList){
            if (pet.getQ().getID().equals(this.getID())){
                pet.getP().setStop(true);

                //globalMatchedList.remove(pet);
            }
        }
        //interrupting the current producer's thread
        Set<Thread> setOfThread = Thread.getAllStackTraces().keySet();

        for(Thread thread : setOfThread){
            if(thread.getId()==this.getCurrentPET().getP().getCurrentThreadID()){
                thread.interrupt();
            }
        }
        this.setActive(false);
    }

    private void ExportResults(String[][] globalAssignedDS) throws IOException {
        // Initialize a Workbook object
        // workbook object
        XSSFWorkbook workbook = new XSSFWorkbook();

        // spreadsheet object
        XSSFSheet spreadsheet
                = workbook.createSheet("AQuACEPDQP");
        // creating a row object
        XSSFRow row;

        //int rowid = 0;

        for (int j=0;j<globalAssignedDS.length;j++){
            Location loc = new Location();
            loc.parseLocation(this.getGlobalAssignedDSByIndex(j,0));
            row = spreadsheet.createRow(j);
            Cell cell1 = row.createCell(0);
            cell1.setCellValue(loc.getLatitude());
            Cell cell2 = row.createCell(1);
            cell2.setCellValue(loc.getLongitude());
            Cell cell3 = row.createCell(2);
            cell3.setCellValue(this.getGlobalAssignedDSByIndex(j,1));


        }



        // .xlsx is the format for Excel Sheets...
        // writing the workbook into the file...

        FileOutputStream out = new FileOutputStream(
                new File("/home/majidlotfian/Downloads/dynamicQP/src/main/resources/resultsQ5.xlsx"));

        workbook.write(out);
        out.close();
        System.out.println("data for query "+this.getID()+" has been exported");



    }


    public Consumer getConsumer(ArrayList<Consumer> consumerList){
        Consumer newConsumer = new Consumer();
        for (Consumer c: consumerList) {
            if (this.getConsumerID().equals(c.getID())){
                newConsumer = c;
                break;
            }
        }

        return newConsumer;
    }

    public void setQualityMetricsWeights(){
       // if (this.getQualityPolicy().isEmpty()){
            //this.wAccuracy = 0.25;
            this.wEnergyConsumption = 1.0;
            //this.wReuseFactor=0.5;
            //this.wLossRate = 0.25;
            //this.wLatency=0.25;
        /*
        }
        if (this.getQualityPolicy().size()==1){
            if (this.getQualityPolicy().get(0).getQualityMetric().getMetricName().equals("Accuracy")){
                //this.wAccuracy = 0.40;
                this.wEnergyConsumption = 0.20;
                //this.wLossRate = 0.20;
                //this.wLatency=0.20;
            }
            if (this.getQualityPolicy().get(0).getQualityMetric().getMetricName().equals("EnergyConsumption")){
                //this.wAccuracy = 0.20;
                this.wEnergyConsumption = 0.40;
                //this.wLossRate = 0.20;
                //this.wLatency=0.20;
            }
            if (this.getQualityPolicy().get(0).getQualityMetric().getMetricName().equals("LossRate")){
                //this.wAccuracy = 0.20;
                this.wEnergyConsumption = 0.20;
                //this.wLossRate = 0.40;
                //this.wLatency=0.20;
            }
            if (this.getQualityPolicy().get(0).getQualityMetric().getMetricName().equals("Latency")){
                //this.wAccuracy = 0.20;
                this.wEnergyConsumption = 0.20;
                //this.wLossRate = 0.20;
                //this.wLatency=0.40;
            }
        }
        //it is also possible to add more if_statements for more than one quality policy


         */
    }
}
