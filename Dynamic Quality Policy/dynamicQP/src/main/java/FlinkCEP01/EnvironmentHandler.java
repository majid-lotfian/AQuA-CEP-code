package FlinkCEP01;


import java.util.*;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Properties;

public class EnvironmentHandler extends JFrame implements ActionListener {


    private Container containerE;

    private JButton addQuery, addProducer, querysList, producersList, consumersList;



    public EnvironmentHandler(){
        setTitle("Environment Handler");
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setSize(200, 300);
        setLocationRelativeTo(null);
        setResizable(false);
        containerE = this.getContentPane();
        containerE.setLayout(new GridLayout(0,1));




        addQuery = new JButton("Add a Query");
        //addQuery.setLocation(20, 120);
        //addQuery.setSize(150, 50);
        addQuery.addActionListener(this);
        containerE.add(addQuery);
        //add(addQuery);

        addProducer = new JButton("Add a Producer");
        //addProducer.setLocation(100, 20);
        //addQuery.setSize(150, 50);
        addProducer.addActionListener(this);
        containerE.add(addProducer);
        //add(addProducer);

        querysList = new JButton("List of Current queries");
        //addProducer.setLocation(100, 20);
        //addQuery.setSize(150, 50);
        querysList.addActionListener(this);
        containerE.add(querysList);

        producersList = new JButton("List of Current producers");
        //addProducer.setLocation(100, 20);
        //addQuery.setSize(150, 50);
        producersList.addActionListener(this);
        containerE.add(producersList);

        consumersList = new JButton("List of Current consumers");
        //addProducer.setLocation(100, 20);
        //addQuery.setSize(150, 50);
        consumersList.addActionListener(this);
        containerE.add(consumersList);

        setVisible(true);

    }


    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource()==addProducer){

            PJFrame newProducer = new PJFrame();
        }
        if (e.getSource()==addQuery){
            QJFrame newQFrame = new QJFrame();
        }
        if (e.getSource() == querysList){
            ArrayList<Object> objects = new ArrayList<>();
            for (Query q: Controller.queryList) {
                objects.add(q);
            }
            ShowList newShowList = new ShowList(objects);
        }
        if (e.getSource() == producersList){
            ArrayList<Object> objects = new ArrayList<>();
            for (Producer p: Controller.producerList) {
                objects.add(p);
            }
            ShowList newShowList = new ShowList(objects);
        }
        if (e.getSource() == consumersList){
            ArrayList<Object> objects = new ArrayList<>();
            for (Consumer c: Controller.consumerList) {
                objects.add(c);
            }
            ShowList newShowList = new ShowList(objects);
        }
    }
}
