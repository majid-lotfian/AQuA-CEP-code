package FlinkCEP01;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;

public class QMA implements Serializable {




    public QMA(){}
    //boolean qmaCheck=false;

    public boolean check(DataEvent de, ProducerEventTypeMatch pet) throws InterruptedException, IOException {
        //double uncertaintyValue =(Double.parseDouble(de.getValue())/Double.parseDouble(de.getGroundTruth()))*100;

        pet.getQ().setGlobalAssignedDS(pet.getQ().getQueryOffset(),0,de.getValue());
        pet.getQ().setGlobalAssignedDS(pet.getQ().getQueryOffset(),1, de.getProducerID());
        Location loc = new Location();
        loc.parseLocation(de.getValue());
        if (!pet.getP().inCoverage(loc)){
            System.out.println("the target goes out of producer "+pet.getP().getID()+" coverage");
            ControlEvent controlEvent = new ControlEvent(pet);
            controlEvent.changeProducer();
            return true;
        } else if (!Controller.MeetAllQP(pet)) {
            System.out.println("producer "+pet.getP().getID()+" does not meet current quality policy!");
            ControlEvent controlEvent = new ControlEvent(pet);
            controlEvent.changeProducer();
            return true;
        }
        for (Producer p:Controller.producerList
             ) {
            if (p.inCoverage(loc) && !p.inCoverage(pet.getQ().getEtList().get(0).getPreviousLocation())){
                System.out.println("The target enters the coverage area of producer "+p.getID());
                ControlEvent controlEvent = new ControlEvent(pet);
                controlEvent.changeProducer();
                return true;
            }
        }


        return false;
    }


    /*public void check(DataStream<DataEvent> dataStream, Query query) throws InterruptedException {



            dataStream
                    .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(query.getWindowSize()), Time.seconds(1)))
                    .process(new ProcessAllWindowFunction<DataEvent, String, TimeWindow>() {
                        @Override
                        public void process(Context context, Iterable<DataEvent> input,
                                            Collector<String> out) throws Exception {
                            //ProducerEventTypeMatch localPET = query.getEtList().get(0).getCurrentPET();
                            //System.out.println("offset in localpet(getoffset) : "+localPET.getP().getOffset());
                            //System.out.println("offset in localpet(offset) : "+localPET.getP().offset);
                            //System.out.println("current producer is : "+Controller.currentPET.getP().getID());
                            int count = 0;
                            for (DataEvent de : input) {
                                count++;
                            }
                            //System.out.println("windowIndex : "+Controller.windowIndex+" - averaging : "+Controller.averaging);
                            for (Query q:Controller.queryList) {
                                if (q.getID().equals(query.getID())){
                                    if (q.getQueryOffset() <= query.getDuration() && q.isActive()) {
                                        out.collect("Window: " + context.window() + "count: " + count);
                                        System.out.println("System Window: " + context.window() + "count: " + count);
                                        q.setGlobalWindowCounts(q.getQueryOffset()-1,0,count);
                                        q.setGlobalWindowCounts(q.getQueryOffset()-1,1,Integer.parseInt(q.getCurrentPET().getP().getID()));
                                        //Controller.GlobalWindowCounts[query.getQueryOffset()][0]=count;
                                        //Controller.GlobalWindowCounts[query.getQueryOffset()][1]=Integer.parseInt(Controller.currentPET.getP().getID());
                                        //System.out.println("local :"+localPET.getP().getLossRate()+" global : "+Controller.currentProducerLossRate);
                                        //System.out.println("producer offset : "+Controller.producerOffset);
                                        //System.out.println("producer offset : "+q.getProducerOffset());
                                        //System.out.println("query offset in qma : "+q.getQueryOffset());
                                        if ( q.getProducerOffset()> q.getWindowSize()-1){


                                            //System.out.println("count :"+count+" current loss rate : "+q.getCurrentProducerLossRate()+
                                                  //  " window index : "+q.getWindowIndex());

                                            if (count < (1-(q.getCurrentProducerLossRate()/100))*q.getWindowSize() && q.getWindowIndex()==1){

                                                q.setAveraging(true);
                                                System.out.println("start averaging ... !");
                                            }

                                            if (q.isAveraging()){

                                                if (q.getWindowIndex() == Controller.maxNumOfWindows){
                                                    q.setWindowEventCounts(q.getWindowIndex()-1,count);
                                                    //Controller.windowEventCounts[Controller.windowIndex-1]=count;
                                                    int validWindows=0;
                                                    String stringSum = "";
                                                    double sum = 0.0;
                                                    for (int i=0; i<Controller.maxNumOfWindows;i++){
                                                        sum+= q.getWindowEventCountsByIndex(i);
                                                        //sum+= Controller.windowEventCounts[i];
                                                        stringSum += " " + q.getWindowEventCountsByIndex(i);
                                                        if (q.getWindowEventCountsByIndex(i)>(1-(q.getCurrentProducerLossRate()/100))*q.getWindowSize()){
                                                            validWindows ++;
                                                        }

                                                    }

                                                    double avg = sum / Controller.maxNumOfWindows;
                                                    //System.out.println("avg is : "+avg);
                                                    //System.out.println("string sum is : "+stringSum);


                                                    double validWindowRatio = (double) validWindows / Controller.maxNumOfWindows;

                                                    if (validWindowRatio < Controller.correctWindowthreshold){
                                                        System.out.println("the producer's loss rate is degraded ...!");

                                                        //System.out.println("Checking all producers !");


                                                        //currentPet.getP().setInUse(0);
                                                        //System.out.println("best score producer : "+FindMaxQScore(Controller.producerList,query));
                                                        //System.out.println("current producer id : "+currentPet.getP().getID());
                                                        //if (!FindMaxQScore(Controller.producerList,query).equals(currentPet.getP().getID())) {
                                                        System.out.println("Changing the producer !");
                                                        //System.out.println("producer offset : "+Controller.producerOffset);
                                                        qmaCheck = true;
                                                        //System.out.println("qma check in qma : "+qmaCheck);
                                                        ControlEvent controlEvent = new ControlEvent(q.getCurrentPET());
                                                        double newLossRate = (1-(avg / q.getWindowSize()))*100;
                                                        controlEvent.changeProducer(newLossRate,q);

                                                        //}
                                                        //else {
                                                        //System.out.println("there is no better producer, nothing changes .. !");
                                                        //}
                                                    }

                                                    q.setWindowIndex(1);
                                                    q.setAveraging(false);
                                                    //Controller.averaging = false;
                                                    for (int j = 0; j<Controller.maxNumOfWindows; j++){
                                                        q.setWindowEventCounts(j,0);
                                                        //Controller.windowEventCounts[j]=0;

                                                    }
                                            /*
                                            System.out.println("the producer's loss rate is degraded ...!");

                                            System.out.println("Checking all producers !");

                                            currentPet.getP().setInUse(0);
                                            System.out.println("best score producer : "+FindMaxQScore(Controller.producerList,query));
                                            System.out.println("current producer id : "+currentPet.getP().getID());
                                            if (!FindMaxQScore(Controller.producerList,query).equals(currentPet.getP().getID())) {
                                                System.out.println("Changing the producer !");
                                                System.out.println("producer offset : "+Controller.producerOffset);

                                                ControlEvent controlEvent = new ControlEvent(currentPet);
                                                controlEvent.changeProducer();

                                            }
                                            else {
                                                System.out.println("there is no better producer, nothing changes .. !");
                                            }


                                                }
                                                else{
                                                    q.setWindowEventCounts(q.getWindowIndex()-1,count);
                                                    //Controller.windowEventCounts[query.getWindowIndex()-1]=count;

                                                    q.setWindowIndex(q.getWindowIndex()+1);
                                                }


                                            }
                                        }

                                    }//else {


                                    //System.out.println("new query offset : "+q.getQueryOffset());
                                }
                            }

                                //Controller.terminateQuery(query);
                           //}
                        }

                    });


    }*/



}
