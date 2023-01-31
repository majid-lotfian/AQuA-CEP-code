package FlinkCEP01;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;

public class QMA implements Serializable {




    public QMA(){}
    boolean qmaCheck=false;


    public void check(DataStream<DataEvent> dataStream, Query query, int window_size) throws InterruptedException {



            dataStream
                    .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(window_size), Time.seconds(1)))
                    .process(new ProcessAllWindowFunction<DataEvent, String, TimeWindow>() {
                        @Override
                        public void process(Context context, Iterable<DataEvent> input,
                                            Collector<String> out) throws Exception {
                            ProducerEventTypeMatch localPET = query.getEtList().get(0).getCurrentPET();
                            //System.out.println("offset in localpet(getoffset) : "+localPET.getP().getOffset());
                            //System.out.println("offset in localpet(offset) : "+localPET.getP().offset);
                            //System.out.println("current producer is : "+Controller.currentPET.getP().getID());
                            int count = 0;
                            for (DataEvent de : input) {
                                count++;
                            }
                            //System.out.println("windowIndex : "+Controller.windowIndex+" - averaging : "+Controller.averaging);

                            if (Controller.queryOffset < query.getDuration()) {
                                out.collect("Window: " + context.window() + "count: " + count);
                                System.out.println("System Window: " + context.window() + "count: " + count);
                                Controller.GlobalWindowCounts[Controller.queryOffset][0]=count;
                                Controller.GlobalWindowCounts[Controller.queryOffset][1]=Integer.parseInt(query.getEtList().get(0).getCurrentPET().getP().getID());
                                //System.out.println("local :"+localPET.getP().getLossRate()+" global : "+Controller.currentProducerLossRate);
                                //System.out.println("producer offset : "+Controller.producerOffset);
                                /*if ( Controller.producerOffset> window_size-1){


                                    if (count < (1-(localPET.getP().getLossRate()/100))*window_size && Controller.windowIndex==1){
                                        Controller.averaging = true;
                                        System.out.println("start averaging ... !");
                                    }

                                    if (Controller.averaging){

                                        if (Controller.windowIndex == Controller.maxNumOfWindows){
                                            Controller.windowEventCounts[Controller.windowIndex -1]=count;
                                            double sum = 0.0;
                                            String stringSum = "";
                                            for (int i = 0; i<Controller.maxNumOfWindows; i++){
                                                sum+= Controller.windowEventCounts[i];
                                                stringSum += " " + Controller.windowEventCounts[i];
                                            }
                                            double avg = sum / Controller.maxNumOfWindows;
                                            //System.out.println("avg is : "+avg);
                                            //System.out.println("string sum is : "+stringSum);
                                            if (avg < ((1-(localPET.getP().getLossRate()/100))*window_size)){
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
                                                ControlEvent controlEvent = new ControlEvent(Controller.currentPET);
                                                double newLossRate = (1-(avg / Controller.windowSize))*100;
                                                controlEvent.changeProducer(newLossRate);

                                                //}
                                                //else {
                                                //System.out.println("there is no better producer, nothing changes .. !");
                                                //}
                                            }

                                            Controller.windowIndex =1;
                                            Controller.averaging = false;
                                            for (int j = 0; j<Controller.maxNumOfWindows; j++){
                                                Controller.windowEventCounts[j]=0;

                                            }

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
                                            Controller.windowEventCounts[Controller.windowIndex -1]=count;

                                            Controller.windowIndex++;
                                        }


                                    }
                                }*/

                            }
                        }

                    });


    }

    private String FindMaxQScore(ArrayList<Producer> producerList, Query query) {
        double max = 0.0;
        String best="";
        for (Producer p : producerList){
            ProducerEventTypeMatch pet = new ProducerEventTypeMatch(p,query.getEtList().get(0),query);
            pet.setNqp(Controller.CountNqp(p,query));
            System.out.println("nqp for list ("+p.getID()+"): "+Controller.CountNqp(p,query)+" max : "+Controller.MaxNqp(Controller.producerList,pet));
            if (Controller.CountNqp(p,query)==Controller.MaxNqp(Controller.producerList,pet)){
                System.out.println("QScore for "+p.getID()+" : "+Controller.CalculateQScore(pet));
                if (Controller.CalculateQScore(pet)>max){
                    max = Controller.CalculateQScore(pet);
                    best =pet.getP().getID();
                }
            }

        }
        return best;
    }
    public boolean QMACheck(){
        if (qmaCheck == true){
            return true;
        }
        return false;
    }

}
