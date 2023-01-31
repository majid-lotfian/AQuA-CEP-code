package FlinkCEP01;



import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

public class QJFrame extends JFrame implements ActionListener {

    private Container container;
    private JLabel title, queryIDLabel, queryDefinitionLabel, queryQualityPolicyLabel, consumerIDLabel, durationLabel, patternDurationLabel;
    private JTextField queryIDTextField, queryDefinitionTextField, queryQualityPolicyTextField, consumerIDTextField, durationTextField, patternDurationTextField;
    private JButton submit, reset;

    public QJFrame(){
        setTitle("Query Submission Form");
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setSize(500, 400);
        setLocationRelativeTo(null);
        setResizable(false);


        container = this.getContentPane();
        container.setLayout(new GridLayout(0,1));

        title = new JLabel("Submit your query:");
        title.setLocation(200,50);
        container.add(title);

        queryIDLabel = new JLabel("Query ID:");
        queryIDLabel.setLocation(50,100);
        container.add(queryIDLabel);

        queryIDTextField = new JTextField();
        queryIDTextField.setLocation(150,100);
        container.add(queryIDTextField);

        queryDefinitionLabel = new JLabel("Definition:");
        queryDefinitionLabel.setLocation(50,150);
        container.add(queryDefinitionLabel);

        queryDefinitionTextField = new JTextField();
        queryDefinitionTextField.setLocation(150,150);
        container.add(queryDefinitionTextField);

        queryQualityPolicyLabel = new JLabel("Quality Policies:");
        queryQualityPolicyLabel.setLocation(50,200);
        container.add(queryQualityPolicyLabel);

        queryQualityPolicyTextField = new JTextField();
        queryQualityPolicyTextField.setLocation(150,200);
        container.add(queryQualityPolicyTextField);

        consumerIDLabel = new JLabel("Consumer ID:");
        consumerIDLabel.setLocation(50,250);
        container.add(consumerIDLabel);

        consumerIDTextField = new JTextField();
        consumerIDTextField.setLocation(150,250);
        container.add(consumerIDTextField);

        durationLabel = new JLabel("Query Duration (in second):");
        durationLabel.setLocation(50,300);
        container.add(durationLabel);

        durationTextField = new JTextField();
        durationTextField.setLocation(150,300);
        container.add(durationTextField);

        /*
        patternDurationLabel = new JLabel("window size :");
        patternDurationLabel.setLocation(50,300);
        container.add(patternDurationLabel);

        patternDurationTextField = new JTextField();
        patternDurationTextField.setLocation(150,300);
        container.add(patternDurationTextField);

         */


        submit = new JButton("Submit");
        submit.setLocation(100, 350);
        submit.addActionListener(this);
        container.add(submit);

        reset = new JButton("Reset");
        reset.setLocation(200, 350);
        reset.addActionListener(this);
        container.add(reset);

        setVisible(true);

    }


    @Override
    public void actionPerformed(ActionEvent e) {
        boolean exists = false;
        if (e.getSource()==submit){
            Query newQuery = new Query();
            newQuery.setActive(true);
            newQuery.setIssueTime((int)(System.currentTimeMillis()/100));
            for (Consumer c: Controller.consumerList) {
                if (c.ID == consumerIDTextField.getText()){
                    exists = true;
                    break;
                }
            }

            if (exists == false){
                Consumer newConsumer = new Consumer(consumerIDTextField.getText());

                Controller.consumerList.add(newConsumer);
            }

            //System.out.println("initializing the query fields");

            newQuery.setID(queryIDTextField.getText());
            newQuery.setConsumerID(consumerIDTextField.getText());
            newQuery.setEtList(ParseQueryDefinition(queryDefinitionTextField.getText(),queryIDTextField.getText()));
            newQuery.setQualityPolicy(ParseQualityPolicy(queryQualityPolicyTextField.getText()));
            newQuery.setDuration(Integer.parseInt(durationTextField.getText()));
            //newQuery.setPatternDuration(Integer.parseInt(patternDurationTextField.getText()));
            //newQuery.setWindowSize(Integer.parseInt(patternDurationTextField.getText()));
            newQuery.setStartTime(System.currentTimeMillis());
            newQuery.setQualityMetricsWeights();
            newQuery.setQueryOffset(0);
            newQuery.setProducerOffset(0);
            //newQuery.GlobalWindowCounts = new int[Integer.parseInt(durationTextField.getText())][2];
            //Producer.InitializeErrors(newQuery.getDuration());

            //System.out.println("query starts in: "+ newQuery.getStartTime());
            //RJFrame newRunQuery = new RJFrame(newQuery);


            Controller.queryList.add(newQuery);
            for (Query q:Controller.queryList) {
                System.out.println("query id is : "+q.getID());
            }
            //System.out.println();

            queryIDTextField.setText("");
            queryDefinitionTextField.setText("");
            queryQualityPolicyTextField.setText("");
            consumerIDTextField.setText("");
            durationTextField.setText("");
            //patternDurationTextField.setText("");

            SwingWorker sw = new SwingWorker() {
                @Override
                protected Object doInBackground() throws Exception {
                    try {
                        //System.out.println("starting the assigndatasource thread");
                        Controller.AssignedDataSource(Controller.producerList, Controller.queryList);

                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                    return null;
                }

                @Override
                protected void done() {

                }
            };
            SwingWorker sw2 = new SwingWorker() {
                @Override
                protected Object doInBackground() throws Exception {

                    //System.out.println("starting the placementGraph thread");
                    Controller.PlaceOperatorGraph(newQuery);

                    return null;
                }

                @Override
                protected void done() {

                }
            };
            sw2.execute();
            sw.execute();


        }
        if (e.getSource()==reset){
            queryIDTextField.setText("");
            queryDefinitionTextField.setText("");
            queryQualityPolicyTextField.setText("");
            consumerIDTextField.setText("");
            durationTextField.setText("");
            //patternDurationTextField.setText("");
        }
    }
    public ArrayList<SE> ParseQueryDefinition(String s, String queryID){

        ArrayList<SE> seList = new ArrayList<>();

        String[] argsQ = s.split(",");
        //System.out.println(argsQ[0]);

        SE se = null;
        for (int k=0; k<argsQ.length; k++){


            String[] argsSE = argsQ[k].split("\\s+");

            for (int j=0; j<argsSE.length; j++){


                if (j%4 == 0){
                    se = new SE();
                    se.setQueryID(queryID);
                    se.setPositionInPattern(k+1);
                    se.setEventType(argsSE[j]);
                }

                if (j%4 == 1){
                    Location location = new Location();
                    location.parseLocation(argsSE[j]);
                    se.setDALocation(location);
                    se.setCurrentLocation(new Location(380,140));
                }

                if (j%4 == 2){
                    se.setOperator(argsSE[j]);
                }

                if (j%4 == 3){
                    se.setValue(argsSE[j]);
                    seList.add(se);
                }

            }


        }
        return seList;
    }
    public ArrayList<QualityPolicy> ParseQualityPolicy(String s){
        ArrayList<QualityPolicy> qpList = new ArrayList<>();
        QualityPolicy qualityPolicy;
        QualityMetric qualityMetric;


        String[] QPStrigList = s.split(",");
        for (String sQP : QPStrigList) {
            qualityPolicy = new QualityPolicy();
            qualityMetric = new QualityMetric();
            QPCondition qpCondition = new QPCondition();

            String[] QPParts = sQP.split(";");

            String[] argsQPCondition = QPParts[1].split("\\s+");
            for (int j=0;j<argsQPCondition.length;j++){
                if (j%3==0){
                    qpCondition.setCriterion(argsQPCondition[j]);
                }

                if (j%3==1){
                    qpCondition.setMinThreshold(Integer.parseInt(argsQPCondition[j]));
                }

                if (j%3==2){
                    qpCondition.setMaxThreshold(Integer.parseInt(argsQPCondition[j]));
                }

            }


            String[] argsQP = QPParts[0].split("\\s+");
            for (int i=0; i<argsQP.length; i++){

                if (i%4 == 0){
                    qualityMetric.setMetricName(argsQP[i]);
                }

                if (i%4 == 1){
                    qualityPolicy.setType(argsQP[i]);
                }

                if (i%4 == 2){
                    qualityMetric.setOperator(argsQP[i]);
                }

                if (i%4 == 3){
                    qualityMetric.setMetricThreshold(Double.parseDouble(argsQP[i]));
                }

            }
            qualityPolicy.setQualityMetric(qualityMetric);
            qualityPolicy.setQpCondition(qpCondition);
            qpList.add(qualityPolicy);
        }

        return qpList;
    }
}
