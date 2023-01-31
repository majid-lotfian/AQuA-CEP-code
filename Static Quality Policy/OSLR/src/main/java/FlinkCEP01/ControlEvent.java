package FlinkCEP01;

import java.io.Serializable;

public class ControlEvent implements Serializable {
    ProducerEventTypeMatch pet;
    public ControlEvent(ProducerEventTypeMatch pet){
        this.pet = pet;
    }

    public void changeProducer(double newLossRate) throws InterruptedException {
        Controller.globalNumberOfLostEvents = Controller.producerLostEvents;
        System.out.println("Producer "+pet.getP().getID()+" has lost "+Controller.producerLostEvents+" events");
        Controller.producerLostEvents =0;

        //Controller.stopSendingData = true;
        //Producer.producerStop(pet);
        Controller.globalMatchedList.remove(pet);
        pet.getP().setStop(true);
        //System.out.println("control event : "+pet.getP().isStop());
        pet.getP().setLossRate(newLossRate);
        Controller.producerOffset = 0;
        System.out.println("LossRate for Producer "+pet.getP().getID()+" is changed to "+newLossRate);
        Controller.AssignedDataSource(Controller.producerList, Controller.queryList);
    }
}
