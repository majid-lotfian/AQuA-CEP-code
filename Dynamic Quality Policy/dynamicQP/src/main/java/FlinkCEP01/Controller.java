package FlinkCEP01;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.util.Collector;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.chocosolver.solver.Model;
import org.chocosolver.solver.Solution;
import org.chocosolver.solver.Solver;
import org.chocosolver.solver.search.strategy.Search;
import org.chocosolver.solver.search.strategy.selectors.values.IntDomainMiddle;
import org.chocosolver.solver.search.strategy.selectors.variables.AntiFirstFail;
import org.chocosolver.solver.search.strategy.selectors.variables.FirstFail;
import org.chocosolver.solver.search.strategy.selectors.variables.Smallest;
import org.chocosolver.solver.search.strategy.selectors.variables.VariableSelectorWithTies;
import org.chocosolver.solver.variables.BoolVar;
import org.chocosolver.solver.variables.IntVar;
import org.chocosolver.util.tools.ArrayUtils;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;


public class Controller {


    public static long executionStart;

    public static int maxNumOfWindows = 10;


    public static double correctWindowthreshold = 80.0;
    public static int windowsInARow = maxNumOfWindows;

    public static QMA qma = new QMA();


    public static Broker broker1;

    public static ArrayList<ProducerEventTypeMatch> globalMatchedList =new ArrayList<>();
    public static ArrayList<ProducerEventTypeMatch> newPETList;
    public static ArrayList<ProducerEventTypeMatch> matchedList;

    public static ArrayList<ArrayList<ProducerEventTypeMatch>> solutionSpace;


    public static ArrayList<Producer> producerList= new ArrayList<>();
    public static ArrayList<Consumer> consumerList = new ArrayList<>();
    public static ArrayList<Query> queryList = new ArrayList<>();
    public static ArrayList<NewTopic> kafkaTopics = new ArrayList<>();
    //public static List<String> GroundTruthTemperature=new ArrayList<>();
    public static List<Point> routeP1 = new ArrayList<>();


    public static boolean stopSendingData = false;
    public static final int NumberofDynamicLossRate = 5;
    public static int[] DynamicLossRatePosition;

    public static final int NumberofDynamicLatency = 5;
    public static int[] DynamicLatencyPosition;

    public static double LossRatePenalty = 2.0;
    public static int LatencyPenalty = 50;
    public static int receivedEventCount =0;
    public static Properties prop_general;
    //public static final int dataArraySize = 100;
    //public static final double ErrorRate = 0.2;
    //public static int[] ErrorPosition;

    public static void main(String[] args) throws Exception {


        broker1 = new Broker("127.0.0.1:9092");
        prop_general = new Properties();
        prop_general.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker1.getConfig());
        prop_general.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop_general.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        initializeEnvironment();
        SwingUtilities.invokeLater(() -> {
            executionStart = System.currentTimeMillis();
            EnvironmentHandler eh = new EnvironmentHandler();


        });



    }

    private static void initializeEnvironment() throws IOException {

        //obtaining input bytes from a file
        FileInputStream fis=new FileInputStream(new File("/home/majidlotfian/Downloads/dynamicQP/src/main/resources/routeP1.xlsx"));
        //creating workbook instance that refers to .xls file
        XSSFWorkbook wb=new XSSFWorkbook(fis);
        //creating a Sheet object to retrieve the object
        XSSFSheet sheet=wb.getSheetAt(0);
        //evaluating cell type
        FormulaEvaluator formulaEvaluator=wb.getCreationHelper().createFormulaEvaluator();
        Point point=new Point();
        int x = 0,y=0;
        for(Row row: sheet)     //iteration over row using for each loop
        {
            if (row.getRowNum()>=0){
                for(Cell cell: row)    //iteration over cell using for each loop
                {
                    if (cell.getColumnIndex()==0){
                        //System.out.println(cell.getNumericCellValue());

                        x = (int) cell.getNumericCellValue();
                    }
                    if (cell.getColumnIndex()==1){
                        //System.out.println(cell.getNumericCellValue());

                        y = (int) cell.getNumericCellValue();
                    }
                }
                routeP1.add(new Point(x,y));
            }


        }

        //GroundTruthTemperature = Files.readAllLines(Paths.get("/home/majidlotfian/code/GroundTruth"));




        int lineNumber=1;
        Producer p=null;
        List<String> s = Files.readAllLines(Paths.get("/home/majidlotfian/Downloads/dynamicQP/src/main/resources/initialize"));

        for (String line : s) {

            if (lineNumber%6 == 1){
                p=new Producer();
                p.setID(line);

            }
            if (lineNumber%6 == 2){
                p.setType(line);

            }
            if (lineNumber%6 == 3){
                Location l = new Location();
                l.parseLocation(line);

                p.setLoc(l);


            }
            if (lineNumber%6 == 4){
                p.setSensingInterval(Integer.parseInt(line));
            }
            if (lineNumber%6 == 5){
                p.setCoverage(Integer.parseInt(line));

            }

            if (lineNumber%6 == 0){
                p.setQmList(PJFrame.ParseQM(line));

                producerList.add(p);


            }
            lineNumber++;

        }

    }

    public static void AssignedDataSource(ArrayList<Producer> producerList, ArrayList<Query> queryList) throws InterruptedException, IOException {

        solutionSpace = new ArrayList<>();

        //int SENumber = 0;

        if (queryList.isEmpty()){
            System.out.println("There is no query");
        }else {
            for (Query q:queryList) {

                for (SE se:q.getEtList()){

                    se.setQueryID(q.getID());
                    //int maxNQP=0;
                    matchedList = new ArrayList<>();
                    //System.out.println("current location se - "+se.getCurrentLocation().getLatitude() + " : "+se.getCurrentLocation().getLongitude());

                    //System.out.println("se type : "+se.getEventType());
                    for (Producer p : producerList) {
                        //System.out.println("p event type : "+p.getType());

                            if (se.getEventType().equals(p.getType())) {


                                if (p.inCoverage(se.getCurrentLocation())) {
                                    //System.out.println("target is now in the coverage of producer : "+p.getID());

                                    ProducerEventTypeMatch pet = new ProducerEventTypeMatch(p, se, q);
                                    //pet.setNqp(CountNqp(p, q));
                                    //System.out.println("count nqp for producer " + p.getID() + " : " + CountNqp(p, q));
                                    pet.setMatchedTime(System.currentTimeMillis());

                                    if (MeetAllQP(pet)) {
                                        matchedList.add(pet);
                                        //System.out.println("producer : "+p.getID()+ " is eligible");
                                    }
                                }
                            }
                        }


                /*
                if (matchedList.isEmpty()==false){

                    double max=0.0;
                    ProducerEventTypeMatch finalPET = new ProducerEventTypeMatch();
                    for (ProducerEventTypeMatch pet: matchedList) {
                        //pet.getNqp() == MaxNqp(producerList,pet)

                        if (CalculateQScore(pet)>max){
                            System.out.println("Score for "+pet.getP().getID()+" is : "+CalculateQScore(pet));
                            max = CalculateQScore(pet);
                            finalPET = pet;
                        }

                    }
                    if (globalMatchedList.contains(finalPET) == false){
                        newPETList.add(finalPET);
                        for (ProducerEventTypeMatch pet: globalMatchedList) {
                            if (pet.getEt() == finalPET.getEt() && pet.getQ() == finalPET.getQ()){
                                globalMatchedList.remove(pet);
                                globalMatchedList.add(finalPET);
                                break;
                            }
                        }
                        se.setCurrentPET(finalPET);
                        currentProducerLossRate=finalPET.getP().getLossRate();
                        System.out.println("for query : " + finalPET.getQ().getID() + " producer : " + finalPET.getP().getID()
                                +" is selected!");

                    }

                }else {
                    System.out.println("No producer is available matches your constraints, Please rewrite your query");
                    Controller.queryList.remove(q);
                    System.out.println("query "+q.getID()+" is removed");
                }
                */
                    //System.out.println("matchedList size : "+matchedList.size());
                    if (!matchedList.isEmpty()){
                        solutionSpace.add(matchedList);
                    }else {
                        System.out.println("No producer meets the quality policy list for query "+q.getID()+" SE "+se.positionInPattern);
                        System.out.println("Please rewrite query "+q.getID());
                        q.terminateQuery();
                    }

                }
            }
        }


        if (solutionSpace.size()>0) {
            //choco-solver section
            //System.out.println("Entering choco-solver");

            //inputs to choco-solver
            int numOfProducers = producerList.size();
            int numOfSE = solutionSpace.size();
            //System.out.println("solution space size : "+numOfSE);
            //cost (energy consumption) array for using each data source
            int[] producerEC = new int[numOfProducers];
            int pCounter = 0;
            for (Producer p : producerList) {

                    producerEC[pCounter] = (int) p.getEnergyConsumption();
                    //System.out.println("EC for producer "+p.getID()+" is : "+producerEC[pCounter]);

                    pCounter++;


            }

            // A new model instance
            Model model = new Model("DataSourceAssignment");

            // VARIABLES
            // a sensor is either active or idle(zero energy consumption)
            BoolVar[] active = model.boolVarArray("active", numOfProducers);
            // which sensor supplies to a SE
            IntVar[] supplier = new IntVar[numOfSE];
            int SECounter = 1;
            //System.out.println("size of solution space is : "+solutionSpace.size());
            for (ArrayList<ProducerEventTypeMatch> ap : solutionSpace) {
                //System.out.println("Num of pets : "+ap.size());
                int[] eligibleProducers = new int[ap.size()];
                //System.out.println("num of eligibles : "+ap.size());
                int petCounter = 0;
                for (ProducerEventTypeMatch pet : ap) {
                    eligibleProducers[petCounter] = petCounter+1;
                    petCounter++;
                }
                //System.out.println();
                supplier[SECounter - 1] = model.intVar("SE" + SECounter, eligibleProducers);
                /*
                for (int i:eligibleProducers
                     ) {
                    System.out.println("producer "+i+" is eligible");

                }

                 */
                SECounter++;
            }
            // supplying EC cost per sensor
            IntVar[] cost = model.intVarArray("cost", numOfProducers, 374, 1634);
            // Total of all costs
            IntVar totalCost = model.intVar("totalCost", 0, 9999);

            // CONSTRAINTS
            for (int j = 0; j < numOfSE; j++) {
                // a sensor is 'active', if it supplies to a SE
                model.element(model.intVar(1), active, supplier[j], 1).post();

            }


            for (int i = 0; i < numOfProducers; i++) {
                // Compute 'cost' for each sensor
                model.element(cost[i], producerEC, model.intVar(i)).post();

            }


            // calculating the totalCost
            model.scalar(active, producerEC, "=", totalCost).post();

            model.setObjective(Model.MINIMIZE, totalCost);
            Solver solver = model.getSolver();
            solver.setSearch(Search.intVarSearch(
                    new VariableSelectorWithTies<>(
                            new FirstFail(model),
                            new Smallest()),
                    new IntDomainMiddle(false),
                    ArrayUtils.append(supplier, cost, active))
            );
            //solver.showShortStatistics();

            /*
            while(solver.solve()){
                prettyPrint(model, active, numOfProducers, supplier, numOfSE, totalCost);
            }

             */


            solver.limitTime("0.1s");
            Solution best = solver.findOptimalSolution(totalCost, false);
            //System.out.println("best solution : " + best);


            //creating pets from the best solution

            for (Query q : Controller.queryList) {
                for (SE se : q.getEtList()) {
                    GenerateKafkaTopics(q, se);
                    newPETList = new ArrayList<>();
                    ProducerEventTypeMatch pet = new ProducerEventTypeMatch
                            (convertToProducer(solutionSpace), se, q);


                    System.out.println("PET - P : "+pet.getP().getID()+" Q : "+pet.getQ().getID()+" SE : "+pet.getEt().getPositionInPattern());
                    if (globalMatchedList.contains(pet) == false) {
                        //System.out.println("adding to newPetList");
                        newPETList.add(pet);
                        for (ProducerEventTypeMatch gp : globalMatchedList) {
                            if (gp.getEt() == pet.getEt() && gp.getQ() == pet.getQ()) {
                                globalMatchedList.remove(gp);

                                break;
                            }
                        }
                        globalMatchedList.add(pet);
                    }

                    //System.out.println("pet p : "+pet.getP().getID());
                    pet.getQ().setCurrentPET(pet);
                    se.setCurrentPET(pet);
                    if (!pet.getP().getID().equals(pet.getQ().getPreviousProducerID())){
                        q.setProducerOffset(0);
                        if (!q.isFirstAssignment()){
                            System.out.println("Producer "+pet.getQ().getPreviousProducerID()+" was placed in idle mode");
                        }
                        System.out.println("for query " + pet.getQ().getCurrentPET().getQ().getID() + " producer " + pet.getQ().getCurrentPET().getP().getID()
                                + " is selected!");

                        //producerOffset = 0;
                    }
                    else {
                        System.out.println("Producer for query "+pet.getQ().getID()+" does not change.");
                    }
                    pet.getQ().setPreviousProducerID(pet.getP().getID());
                    //System.out.println("loss rate : "+pet.getP().getLossRate());
                    //pet.getQ().setCurrentProducerLossRate(pet.getP().getLossRate());
                    pet.getP().setStop(false);


                }

            }
        }

        //System.out.println("new pet list size : "+newPETList.size());
        if (!newPETList.isEmpty()){
            for (ProducerEventTypeMatch pet: newPETList) {
                SwingWorker swingWorker = new SwingWorker() {
                    @Override
                    protected Object doInBackground() throws Exception {
                        //System.out.println("Starting the producer "+pet.getP().getID());
                        if (pet.getQ().isFirstAssignment()) {
                            //Thread.currentThread().sleep(5000);
                            pet.getQ().setFirstAssignment(false);
                            //firstAssignment = false;
                        }
                        pet.getP().setCurrentThreadID(Thread.currentThread().getId());
                        pet.getP().producerStart(pet);
                        //System.out.println("starting the producer thread");
                        //Producer.producerStart(pet);
                        return null;
                    }
                };


                swingWorker.execute();

            }
        }

    }



    private static int ExtractProducerFromSolution(String best) {
        String[] argsBest = best.split("\\s+");
        for (String s:argsBest) {
            if (s.startsWith("active")) {
                int i = Integer.parseInt(s.substring(s.length() - 2,s.length() - 1));
                if (i==1){
                    return (Integer.parseInt(s.substring(s.length() - 5,s.length() - 4))+1);
                }
                //System.out.println("last character is : " + s.substring(s.length() - 2,s.length() - 1));

            }
        }
        return 0;
    }

    private static void prettyPrint(Model model, IntVar[] active, int NoP, IntVar[] supplier, int NoSE, IntVar totalcost) {
        StringBuilder st = new StringBuilder();
        st.append("Solution #").append(model.getSolver().getSolutionCount()).append("\n");
        for (int i = 0; i < NoP; i++) {
            if (active[i].getValue() > 0) {
                st.append(String.format("\tProducer %d supplies SE : ", (i + 1)));
                for (int j = 0; j < NoSE; j++) {
                    if (supplier[j].getValue() == (i + 1)) {
                        st.append(String.format("%d ", (j + 1)));
                    }
                }
                st.append("\n");
            }
        }
        st.append("\tTotal C: ").append(totalcost.getValue());
        System.out.println(st.toString());
    }
    public static boolean MeetAllQP(ProducerEventTypeMatch pet) {
        //System.out.println("nqp for producer "+pet.getP().getID()+" is: "+pet.getNqp());
        //System.out.println("qp size in query is : "+pet.getQ().getQualityPolicy().size());
        /*
        if (pet.getNqp() == pet.getQ().getQualityPolicy().size()){
            return true;
        }

         */
        double distanceToDA = Math.sqrt(
                Math.pow((6-pet.getQ().getEtList().get(0).getCurrentLocation().getLatitude()),2)+
                        Math.pow((6-pet.getQ().getEtList().get(0).getCurrentLocation().getLongitude()),2));
        if (distanceToDA < 20){
            if (pet.getP().getAccuracy() < 2){
                return true;
            }

        } else if (20<distanceToDA && distanceToDA<80) {
            if (pet.getP().getAccuracy() < 5){
                return true;
            }

        }else {
            if (pet.getP().getAccuracy() < 10){
                return true;
            }

        }
        return false;
    }

    /*public static int CountNqp(Producer p, Query query) {
        int countQP = 0;
        for (QualityPolicy qp: query.getQualityPolicy()) {

            if (p.meetQualityPolicy(qp)){
                //System.out.println("meet "+qp.getQualityMetric().getMetricName());
                countQP++;

            }
        }
        return countQP;
    }*/

    /*public static int MaxNqp(ArrayList<Producer> pList, ProducerEventTypeMatch pet){
        int max = 0;
        for (Producer p : pList){
            if (CountNqp(p,pet.getQ())>max){
                max=CountNqp(p,pet.getQ());
            }
        }
        return max;
    }*/


    private static void GenerateKafkaTopics(Query query , SE se) {
        AdminClient adminClient = AdminClient.create(prop_general);
        NewTopic queryInputTopic = new NewTopic(query.getID()+se.getEventType()+"Topic", 1, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)

        Consumer c1 = new Consumer();
        NewTopic queryOutputTopic = new NewTopic(query.getConsumerID()+ query.getID(), 1, (short)1);
        List<NewTopic> newTopics = new ArrayList<NewTopic>();

        if (!kafkaTopics.contains(queryInputTopic)){
            newTopics.add(queryInputTopic);
        }

        if (!kafkaTopics.contains(queryOutputTopic)){
            newTopics.add(queryOutputTopic);
        }
        adminClient.createTopics(newTopics);
        adminClient.close();
    }

    /*public static double CalculateQScore(ProducerEventTypeMatch pet){
        double ECMax = MaxQualityMetric(producerList,"EnergyConsumption");
        double ECMin = MinQualityMetric(producerList,"EnergyConsumption");
        double LatencyMax = MaxQualityMetric(producerList, "Latency");
        double LatencyMin = MinQualityMetric(producerList, "Latency");
        //System.out.println("Lmax : "+LatencyMax+" Lmin : "+ LatencyMin+" ECmax : "+ECMax+" ECmin : "+ECMin);
        //System.out.println("w A : "+pet.getQ().wAccuracy+" w EC : "+pet.getQ().wEnergyConsumption
        //+" w LR : "+pet.getQ().wLossRate+" w Lat : "+pet.getQ().wLatency);
        //double q1 = pet.getQ().wAccuracy*(pet.getP().getAccuracy()/100);
        //System.out.println("q1 : "+q1);
        //double q2 = pet.getQ().wEnergyConsumption*((ECMax-pet.getP().getEnergyConsumption())/(ECMax-ECMin));
        //System.out.println("q2 : "+q2);
        //double q3 = pet.getQ().wLossRate*(1-(pet.getP().getLossRate()/100));
        //System.out.println("q3 : "+q3);
        //double q4 = pet.getQ().wLatency*((LatencyMax-pet.getP().getLatency())/(LatencyMax-LatencyMin));
        //System.out.println("q4 : "+q4);
        //double q1 = pet.getQ().getwEnergyConsumption()*((ECMax-pet.getP().getEnergyConsumption())/(ECMax-ECMin));
        //System.out.println("energy : "+pet.getQ().getwEnergyConsumption() + " q1 : "+q1+ " reuse : "+pet.getQ().getwReuseFactor());
        double qScore =  pet.getP().getEnergyConsumption();
        //System.out.println("qscore : "+qScore);
        return qScore;
    }*/

    public static void PlaceOperatorGraph(Query query) throws InterruptedException {

        System.out.println("entering the OPG");

        //query pattern detection
        StreamExecutionEnvironment qEnv = StreamExecutionEnvironment.getExecutionEnvironment();



        KafkaSource<String> qSource = KafkaSource.<String>builder()
                .setBootstrapServers(broker1.getConfig())
                .setTopics(query.getID()+query.getEtList().get(0).getEventType()+"Topic")
                .setGroupId("group2")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stringQueryInput = qEnv.fromSource(qSource, WatermarkStrategy.noWatermarks(), "KafkaTopic");



        DataStream<DataEvent> qInput = stringQueryInput.map(new MapFunction<String, DataEvent>() {

            @Override
            public DataEvent map(String s) throws Exception {
                System.out.println("in the data stream");

                DataEvent d = ExtractDataEvent(s);
                return d;
            }
        });


        Pattern<DataEvent, ?> qPattern= Pattern.<DataEvent>begin("start").where(new SimpleCondition<DataEvent>() {
            @Override
            public boolean filter(DataEvent e) throws Exception {
                SE simpleEvent = query.getEtList().get(0);

                System.out.println("value in CE: "+e.getValue());
                return e.getType().equals(simpleEvent.getEventType()) && simpleEvent.checkThreshold(e.getValue()) ;
            }
        });

        /*for (int i = 1; i< newQuery.getEtList().size();i++){
            int finalI = i;
            qPattern = qPattern.followedBy(String.valueOf(i)).where(new SimpleCondition<DataEvent>() {
                @Override
                public boolean filter(DataEvent e) throws Exception {

                    SE de = newQuery.getEtList().get(finalI);
                    return e.getType().equals(de.getEventType()) && SE.checkThreshold(de.getOperator(), e.getValue(),de.getOperator());

                }
            }).oneOrMore();
        }*/

        //qPattern = qPattern.within(Time.seconds(newQuery.getPatternDuration()));


        PatternStream<DataEvent> patternStream = CEP.pattern(qInput, qPattern).inProcessingTime();
        DataStream<DataEvent> result = patternStream.process(new PatternProcessFunction<DataEvent, DataEvent>() {

            @Override
            public void processMatch(Map<String, List<DataEvent>> map, Context context, Collector<DataEvent> collector) throws Exception {


                long latency = (int)(Double.parseDouble(map.get("start").get(0).getSendingLatency()));
                long CETimeStamp = System.currentTimeMillis()+latency;
                //System.out.println("latency : "+latency+" ce :"+CETimeStamp);
                ArrayList<AttributeValue> avc=new ArrayList<AttributeValue>();
                avc.add(new AttributeValue("QueryID", query.getID()));
                avc.add(new AttributeValue("ConsumerID", query.getConsumerID()));
                avc.add(new AttributeValue("CETimeStamp", ""+ CETimeStamp));
                //avc.add(new AttributeValue("event TimeStamp", map.get("start").get(0).getTimeStamp()));
                //avc.add(new AttributeValue("ct milli",""+System.currentTimeMillis()));
                avc.add(new AttributeValue("Point",""+map.get("start").get(0).getValue()));

                DataEvent devent = new DataEvent(avc);


                collector.collect(devent);
            }
        });


        //QMA checks the lost events

        //qma.check(qInput, query);



        Consumer c1 = new Consumer();
        String CTopic = c1.setConsumerTopic(query);



        KafkaSink<String> qSink = KafkaSink.<String>builder()
                .setBootstrapServers(broker1.getConfig())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(CTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        //.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<String> complexEventStream = result.map(new MapFunction<DataEvent, String>() {
            @Override
            public String map(DataEvent dataEvent) throws Exception {
                System.out.println("complex event : "+dataEvent.toString());
                return dataEvent.toString();
            }
        });
        complexEventStream.sinkTo(qSink);


        c1.ConsumerStart(query);


        try {
            qEnv.execute();
        } catch (Exception e) {
            System.out.println("Error happen");
            e.printStackTrace();
        }

    }
    public static Producer convertToProducer (ArrayList<ArrayList<ProducerEventTypeMatch>> solutionSpace){

        /*
        for (Producer producer: producerList) {
            //System.out.println("Producer Id is : "+producer.getID()+ " and p is : "+p);
            if (Integer.parseInt(producer.getID()) == p){
                return producer;
            }
        }

         */
        for (ArrayList<ProducerEventTypeMatch> ap : solutionSpace) {
            Producer lowestEC=new Producer();
            int minEC = 2000;
            for (ProducerEventTypeMatch pet:ap){
                //System.out.println("producer : "+pet.getP().getID()+" - EC : "+(int)pet.getP().getEnergyConsumption());
                if (pet.getP().getEnergyConsumption()<minEC){
                    minEC = (int)pet.getP().getEnergyConsumption();
                    lowestEC = pet.getP();
                }
            }
            //System.out.println("lowest : "+lowestEC.getID()+" - EC : "+minEC);
            return lowestEC;
        }
        return null;
    }
    public static SE extractSE(int supplierPosition){
        int i = 0;
        for (Query q:queryList) {
            for (SE se:q.getEtList()) {
                if (i == supplierPosition){
                    return se;
                }
                i++;
            }
        }
        return null;
    }

    public static Query extractQuery(String queryID){
        for (Query q : queryList){

            if (q.getID().equals(queryID)){
                //System.out.println("query is :"+q.getID()+" and id is : "+queryID);
                return q;
            }
        }

        return null;
    }

    public static double MaxQualityMetric(ArrayList<Producer> producerList, String qualityMetricName){
        double max=0;
        for (Producer p:producerList
        ) {
            for (QualityMetric qm:p.getQmList()
            ) {
                if (qm.getMetricName().equals(qualityMetricName)){
                    if (qm.getMetricThreshold()>max){
                        max = qm.getMetricThreshold();
                    }
                }
            }

        }
        return max;
    }

    public static double MinQualityMetric(ArrayList<Producer> producerList, String qualityMetricName){
        double min=10000;
        for (Producer p:producerList
        ) {
            for (QualityMetric qm:p.getQmList()
            ) {
                if (qm.getMetricName().equals(qualityMetricName)){
                    if (qm.getMetricThreshold()<min){
                        min =  qm.getMetricThreshold();
                    }
                }
            }

        }
        return min;
    }

    public static DataEvent ExtractDataEvent(String string) {
        String[] args = string.split("\\s+");

        ArrayList<AttributeValue> al=new ArrayList<AttributeValue>();

        int i =0;
        AttributeValue av=null;
        for (String s1:args
        ) {

            if (i==1){
                av.setAttributeValue(s1);
                al.add(av);
                i--;
            }else {
                av= new AttributeValue();
                av.setAttributeName(s1);
                i++;
            }

        }

        DataEvent dataEvent=new DataEvent(al);

        return dataEvent;
    }
}

