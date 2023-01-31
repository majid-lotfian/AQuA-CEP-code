package FlinkCEP01;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

public class PJFrame extends JFrame implements ActionListener {

    private Container containerP;
    private JLabel title, producerIDLabel, producerTypeLabel, producerLocationLabel, producerSensingIntervalLabel, producerCoverageLabel, qualityMetricLabel, rangeLabel;
    private JTextField producerIDTextField, producerTypeTextField, producerLocationTextField, producerSensingIntervalTextField, producerCoverageTextField, qualityMetricTextField, rangeTextField;
    private JButton submit, reset, addQM;


    public PJFrame(){
        setTitle("Producer Generation Form");
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setSize(500, 400);
        setLocationRelativeTo(null);
        setResizable(false);


        containerP = this.getContentPane();
        containerP.setLayout(new GridLayout(0,1));

        title = new JLabel("Enter the Producer's information:");
        title.setLocation(150,50);
        containerP.add(title);

        producerIDLabel = new JLabel("Producer ID:");
        producerIDLabel.setLocation(50,100);
        containerP.add(producerIDLabel);

        producerIDTextField = new JTextField();
        producerIDTextField.setLocation(150,100);
        containerP.add(producerIDTextField);

        producerTypeLabel = new JLabel("Event Type:");
        producerTypeLabel.setLocation(50,150);
        containerP.add(producerTypeLabel);

        producerTypeTextField = new JTextField();
        producerTypeTextField.setLocation(150,150);
        containerP.add(producerTypeTextField);

        producerLocationLabel = new JLabel("Location:");
        producerLocationLabel.setLocation(50,200);
        containerP.add(producerLocationLabel);

        producerLocationTextField = new JTextField();
        producerLocationTextField.setLocation(150,200);
        containerP.add(producerLocationTextField);


        producerSensingIntervalLabel = new JLabel("Sensing Interval:");
        producerSensingIntervalLabel.setLocation(50,250);
        containerP.add(producerSensingIntervalLabel);

        producerSensingIntervalTextField = new JTextField();
        producerSensingIntervalTextField.setLocation(150,250);
        containerP.add(producerSensingIntervalTextField);

        producerCoverageLabel = new JLabel("Coverage:");
        producerCoverageLabel.setLocation(50, 300);
        containerP.add(producerCoverageLabel);

        producerCoverageTextField = new JTextField();
        producerCoverageTextField.setLocation(150, 300);
        containerP.add(producerCoverageTextField);

        rangeLabel = new JLabel("Range");
        containerP.add(rangeLabel);

        rangeTextField = new JTextField();
        containerP.add(rangeTextField);

        qualityMetricLabel = new JLabel("Quality Metric:");
        qualityMetricLabel.setLocation(50, 300);
        containerP.add(qualityMetricLabel);

        qualityMetricTextField = new JTextField();
        qualityMetricTextField.setLocation(150, 300);
        containerP.add(qualityMetricTextField);


        submit = new JButton("Submit");
        submit.setLocation(100, 350);
        submit.addActionListener(this);
        containerP.add(submit);

        reset = new JButton("Reset");
        reset.setLocation(200, 350);
        reset.addActionListener(this);
        containerP.add(reset);

        setVisible(true);

    }

    @Override
    public void actionPerformed(ActionEvent e) {
        Location location =new Location();


        if (e.getSource()==submit){
            Producer newProducer = new Producer();
            newProducer.setID(producerIDTextField.getText());
            newProducer.setType(producerTypeTextField.getText());
            newProducer.setLoc(location.parseLocation(producerLocationTextField.getText()));
            newProducer.setSensingInterval(Integer.parseInt(producerSensingIntervalTextField.getText()));
            newProducer.setCoverage(Double.parseDouble(producerCoverageTextField.getText()));
            newProducer.setRange(Double.parseDouble(rangeTextField.getText()));
            newProducer.setQmList(ParseQM(qualityMetricTextField.getText()));
            Controller.producerList.add(newProducer);
            System.out.println("Producer " + newProducer.getID() + " is added!");
            /*for (Producer pr:Controller.producerList) {
                System.out.println("Producer is added in PJFrame" + pr.getID());
            }*/
            producerIDTextField.setText("");
            producerTypeTextField.setText("");
            producerLocationTextField.setText("");
            producerSensingIntervalTextField.setText("");
            producerCoverageTextField.setText("");
            rangeTextField.setText("");
            qualityMetricTextField.setText("");

            try {
                Controller.AssignedDataSource(Controller.producerList, Controller.queryList);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }

        }
        if (e.getSource()==reset){
            producerIDTextField.setText("");
            producerTypeTextField.setText("");
            producerLocationTextField.setText("");
            producerSensingIntervalTextField.setText("");
            producerCoverageTextField.setText("");
            rangeTextField.setText("");
            qualityMetricTextField.setText("");
        }
    }

    public static ArrayList<QualityMetric> ParseQM(String s) {

        ArrayList<QualityMetric> PqmList =new ArrayList<>();
        QualityMetric qualityMetric;

        String[] QMStringList = s.split(",");
        for (String sQM : QMStringList) {

            qualityMetric = new QualityMetric();

            String[] argsQM = sQM.split("\\s+");
            /*for (int i=0; i<argsQM.length;i++){
                System.out.println(argsQM[i]);
            }*/

            for (int i=0; i<argsQM.length; i++){
                if (i%3 == 0){
                    qualityMetric.setMetricName(argsQM[i]);
                }

                if (i%3 == 1){
                    qualityMetric.setOperator(argsQM[i]);
                }

                if (i%3 == 2){
                    //System.out.println(argsQM[i]);
                    qualityMetric.setMetricThreshold(Double.parseDouble(argsQM[i]));
                }

            }

            PqmList.add(qualityMetric);
        }

        return PqmList;
    }
}
