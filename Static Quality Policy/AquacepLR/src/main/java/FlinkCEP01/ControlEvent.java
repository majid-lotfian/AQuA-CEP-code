package FlinkCEP01;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

public class ControlEvent implements Serializable {
    ProducerEventTypeMatch pet;
    public ControlEvent(ProducerEventTypeMatch pet){
        this.pet = pet;
    }

    public void changeProducer(double newLossRate) throws InterruptedException, IOException {
        Controller.globalNumberOfLostEvents = Controller.producerLostEvents;
        System.out.println("Producer "+pet.getP().getID()+" has lost "+Controller.producerLostEvents+" events");
        Controller.producerLostEvents =0;

        //Controller.stopSendingData = true;
        //Producer.producerStop(pet);
        Controller.globalMatchedList.remove(pet);
        pet.getP().setStop(true);
        //System.out.println("control event : "+pet.getP().isStop());
        pet.getP().setLossRate(newLossRate);
        //Controller.producerOffset = 0;
        System.out.println("LossRate for Producer "+pet.getP().getID()+" is changed to "+newLossRate);
        Set<Thread> setOfThread = Thread.getAllStackTraces().keySet();

        //Iterate over set to find yours
        for(Thread thread : setOfThread){
            if(thread.getId()==pet.getP().getCurrentThreadID()){
                thread.interrupt();
                System.out.println("Producer "+pet.getP().getID()+" was placed in idle mode");
            }
        }
        Controller.AssignedDataSource(Controller.producerList, Controller.queryList);
    }
}
