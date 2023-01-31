package FlinkCEP01;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

public class ShowList extends JFrame implements ActionListener {
    private Container container;
    private ArrayList<JLabel> items=new ArrayList<>();


    public ShowList(ArrayList<Object> arrayList) {

        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setSize(500, 400);
        setLocationRelativeTo(null);
        setResizable(false);

        container = this.getContentPane();
        container.setLayout(new GridLayout(3,1));



        for (Object o: arrayList) {
            if (o.getClass()== Query.class){
                this.setTitle("Current List of Queries");
                JLabel j = new JLabel();
                Query q = (Query) o;
                j.setText(q.getID());
                items.add(j);
            }
            if (o.getClass()== Producer.class){
                this.setTitle("Current List of Producers");
                JLabel j = new JLabel();
                Producer p = (Producer) o;
                j.setText(p.getID());
                //System.out.println(p.getID());
                items.add(j);
            }
            if (o.getClass()== Consumer.class){
                this.setTitle("Current List of Consumers");
                JLabel j = new JLabel();
                Consumer c = (Consumer) o;
                j.setText(c.getID());
                items.add(j);
            }
            for (JLabel j: items) {
                container.add(j);
            }
            setVisible(true);
        }




    }


    @Override
    public void actionPerformed(ActionEvent e) {

    }
}
