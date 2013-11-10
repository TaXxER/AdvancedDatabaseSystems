package graph;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.AdjustmentEvent;
import java.awt.event.AdjustmentListener;
import java.text.DateFormat;
import java.util.Date;
import java.util.Hashtable;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollBar;
import javax.swing.JSlider;
import javax.swing.JTextField;
import javax.swing.UIManager;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.DeviationRenderer;
import org.jfree.data.Range;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.YIntervalSeries;
import org.jfree.data.xy.YIntervalSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import aggregation.ManageAggregations;


public class GraphGui extends ApplicationFrame{

	private static class JdbcPanel extends JPanel 
	implements AdjustmentListener, ChangeListener, ActionListener	{

		private static final int step = 10;
		private ValueAxis domainAxis;
		private XYDataset xydataset;
		private static int preferedHeight = 500;
		private static int preferedWidth = 270;
		private boolean preciseMode = false;
		private ManageAggregations aggregations;
		private JdbcYIntervalSeries timeseries;

		private XYDataset createPegelAndelfingen(){
			YIntervalSeriesCollection timeseriescollection = new YIntervalSeriesCollection();
			//			JdbcYIntervalSeries timeseries = new JdbcYIntervalSeries("Pegel Andelfingen",
			//					"jdbc:mysql://silo1.ewi.utwente.nl:3306/group01",
			//					"com.mysql.jdbc.Driver",
			//					"group01", 
			//					"sensor01",
			//					"timed",
			//					"PEGEL",
			//					"pegel_andelfingen2",
			//					null);
			this.timeseries = new JdbcYIntervalSeries("Pegel at Andelfingen",
					"jdbc:mysql://localhost:3306/ads",
					"com.mysql.jdbc.Driver",
					"ads_account", 
					"adsisgaaf",
					"timed",
					"PEGEL",
					"dataset_1",
					null);
			Range range = timeseries.getDomainRange();
			timeseries.update((new Double(range.getLowerBound())).longValue(), (new Double(range.getUpperBound()-range.getLowerBound())).longValue());
			timeseriescollection.addSeries(timeseries);
			
			return timeseriescollection;
		}


		private JFreeChart createChart()
		{
			xydataset = createPegelAndelfingen();
			JFreeChart jfreechart = ChartFactory.createTimeSeriesChart("The Thur Valley", "Date", "Waterheight", xydataset, true, true, false);

			XYPlot xyplot = jfreechart.getXYPlot();
			domainAxis = xyplot.getDomainAxis();
			domainAxis.setLowerMargin(0.0D);
			domainAxis.setUpperMargin(0.0D);
			YIntervalSeries series = ((YIntervalSeriesCollection) xydataset).getSeries(0);
			domainAxis.setRange(Math.floor(series.getX(0).doubleValue()), Math.ceil(series.getX(series.getItemCount()-1).doubleValue()));
			rangeAxis = new NumberAxis("Waterheight");
			DeviationRenderer deviationrenderer = new DeviationRenderer(true, false);
			deviationrenderer.setSeriesStroke(0, new BasicStroke(3F, 1, 1));
			deviationrenderer.setSeriesStroke(0, new BasicStroke(3F, 1, 1));
			deviationrenderer.setSeriesFillPaint(0, new Color(255, 200, 200));
			deviationrenderer.setBaseShapesVisible(true);
			deviationrenderer.setSeriesShapesFilled(0, Boolean.FALSE);
			deviationrenderer.setSeriesShapesFilled(1, Boolean.FALSE);
			xyplot.setRenderer(deviationrenderer);			
			xyplot.setRangeAxis(rangeAxis);
			rangeAxis.setRange(350, 360);
			return jfreechart;
		}

		private JScrollBar scrollbar;
		private NumberAxis rangeAxis;
		private double factorRange;
		private double factorDomain;
		private double extentR;
		private long extentD;
		private JScrollBar scrollbarh;
		private double valueR;
		private long valueD;
		private JSlider quantileSlider;
		private int quantile=0;
		private JTextField textField;
		private JButton toLeft;
		private JButton toRight;
		
		// Own addition of radio button
		private ButtonGroup  modeSelector = new ButtonGroup();
		private JRadioButton fastModeButton;
		private JRadioButton preciseModeButton;

		public JdbcPanel()
		{
			super(new BorderLayout());
			//			degrees = 45D;
			//			JPanel jpanel = new JPanel(new GridLayout(3, 1));
			JPanel jpanel = new JPanel();
			jpanel.setLayout(new BoxLayout(jpanel, BoxLayout.Y_AXIS));

			JFreeChart jfreechart = createChart();
			ChartPanel chartpanel = new ChartPanel(jfreechart);
			chartpanel.setPreferredSize(new Dimension(preferedHeight, preferedWidth));
			Range r = rangeAxis.getRange();
			factorRange = 80.0/r.getLength();
			double diff = r.getLength()*0.2;
			scrollbar = new JScrollBar(JScrollBar.VERTICAL, (int)Math.floor(r.getLowerBound()*factorRange) , 
					(int) Math.ceil(r.getLength()*factorRange), 
					(int) Math.floor(((r.getLowerBound()-diff)*factorRange)), 
					(int) Math.ceil(((r.getUpperBound()+diff)*factorRange)));
			scrollbar.addAdjustmentListener(this);
			
			toLeft = new JButton("<");
			toRight = new JButton(">");
			toLeft.addActionListener(this);
			toRight.addActionListener(this);

			r = domainAxis.getRange();
			valueD = (long) r.getLowerBound();
			extentD = (long) r.getLength();
			factorDomain = 80.0/extentD;
			diff = extentD*0.2;
			scrollbarh = new JScrollBar(JScrollBar.HORIZONTAL, 
					(int)Math.floor(valueD*factorDomain) , 
					(int) Math.ceil(extentD*factorDomain), 
					(int) Math.floor(((valueD-diff)*factorDomain)), 
					(int) Math.ceil(((valueD+extentD+diff)*factorDomain)));
			scrollbarh.addAdjustmentListener(this);

			quantileSlider = new JSlider(JSlider.VERTICAL, 0, 5, quantile);
			quantileSlider.setPaintLabels(true);
			quantileSlider.setPaintTicks(true);
			quantileSlider.setMajorTickSpacing(1);
			//quantileSlider.setMinorTickSpacing(5);
			quantileSlider.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
			quantileSlider.setSnapToTicks(true);
			quantileSlider.setInverted(true);
			Hashtable<Integer, JLabel> table = new Hashtable<Integer, JLabel>();
			table.put (0, new JLabel("0"));
			table.put (1, new JLabel("10"));
			table.put (2, new JLabel("20"));
			table.put (3, new JLabel("30"));
			table.put (4, new JLabel("40"));
			table.put (5, new JLabel("50"));
			quantileSlider.setLabelTable (table);
			quantileSlider.addChangeListener(this);

			// Initialize radiobuttons
			fastModeButton 		= new JRadioButton("fast mode");
			fastModeButton.addActionListener(this);
			fastModeButton.setSelected(true);
			preciseModeButton 	= new JRadioButton("precise mode");
			preciseModeButton.addActionListener(this);
			modeSelector.add(fastModeButton);
			modeSelector.add(preciseModeButton);
			
			jpanel.add(scrollbar);
			add(jpanel, "West");
			add(chartpanel);
//			add(quantileSlider,"East");
			add(scrollbarh,"South");
			JPanel jpanel2 = new JPanel();
			jpanel2.setLayout(new BoxLayout(jpanel2, BoxLayout.Y_AXIS));
			jpanel2.add(scrollbarh);
			JPanel jpanel21 = new JPanel();
//			b.addActionListener(this);
			jpanel21.add(toLeft);
			jpanel21.add(toRight);
//			b.addActionListener(this);
//			textField.addActionListener(this);
			jpanel2.add(jpanel21);
			// Add radiobuttons to GUI
			jpanel2.add(fastModeButton);
			jpanel2.add(preciseModeButton);
			add(jpanel2,"South");

			//add(new JButton("+"),"SouthEast");

		}

		public void adjustmentValueChanged(AdjustmentEvent e) {
			if(e.getSource() == scrollbar)
			{
				valueR = scrollbar.getValue()/factorRange;
				extentR = scrollbar.getVisibleAmount()/factorRange;
				rangeAxis.setRange(valueR, valueR+extentR);
			} else if(e.getSource() == scrollbarh)
			{
				valueD = (long) (scrollbarh.getValue()/factorDomain);
				extentD = (long) (scrollbarh.getVisibleAmount()/factorDomain);
				
				if(extentD == 0){
					extentD = 36000000;
				}
				
				// reload data set
				YIntervalSeriesCollection col = (YIntervalSeriesCollection) xydataset;
				for(int i=0; i<col.getSeriesCount(); i++){
					JdbcYIntervalSeries series = (JdbcYIntervalSeries) col.getSeries(i);
					series.update(valueD, extentD);
				}
				domainAxis.setRange(valueD, valueD+extentD);
			}
		}

		public void stateChanged(ChangeEvent changeevent) {
			if(changeevent.getSource() == quantileSlider)
			{
				quantile = quantileSlider.getValue();
				YIntervalSeriesCollection col = (YIntervalSeriesCollection) xydataset;
				for(int i=0; i<col.getSeriesCount(); i++){
					JdbcYIntervalSeries series = (JdbcYIntervalSeries) col.getSeries(i);
					series.update(valueD, extentD);
				}
			} 
		}
		
		@Override
		public void actionPerformed(ActionEvent ae) {
			if((ae.getSource() == toLeft) || (ae.getSource() == toRight)){
				valueD = (long) (scrollbarh.getValue()/factorDomain);
				extentD = (long) (scrollbarh.getVisibleAmount()/factorDomain);
				
				if(extentD == 0){
					extentD = 36000000;
				}				
				
				// reload data set
				YIntervalSeriesCollection col = (YIntervalSeriesCollection) xydataset;
				for(int i=0; i<col.getSeriesCount(); i++){
					JdbcYIntervalSeries series = (JdbcYIntervalSeries) col.getSeries(i);
					series.update(valueD, extentD);
				}
				
				DateFormat df2 = DateFormat.getDateInstance(DateFormat.MEDIUM);
				
				Date epoch = new Date(valueD);				
				String oldStart = df2.format(epoch);				
				System.out.println("Old start: " + oldStart);
				
				Date epoch2 = new Date(valueD+extentD);				
				String oldEnd = df2.format(epoch2);				
				System.out.println("Old end: " + oldEnd);
				
				
				long newVal;
				
				if(ae.getSource() == toLeft){	
					newVal = valueD-(extentD/2);
				} else {
					newVal = valueD+(extentD/2);
				}				

				
				Date epoch3 = new Date(newVal);				
				String newStart = df2.format(epoch3);				
				System.out.println("New start: " + newStart);
				
				Date epoch4 = new Date(newVal+extentD);				
				String newEnd = df2.format(epoch4);				
				System.out.println("New end: " + newEnd);
				
				scrollbarh.setValue((int)(newVal*factorDomain));
				scrollbarh.setVisibleAmount((int)(extentD*factorDomain));
				
				domainAxis.setRange(newVal, newVal+extentD);
				
			} else if(ae.getSource() == fastModeButton){
				if(preciseMode){
					timeseries.getAggregationManager().setPreciseMode(false);
					preciseMode = false;
				}
			} else if(ae.getSource() == preciseModeButton){
				if(!preciseMode){
					timeseries.getAggregationManager().setPreciseMode(true);
					preciseMode = true;
				}
			}			
		}

	} // end of JdbcPanel class


	public GraphGui(String title){
		super(title);
		setContentPane(new JdbcPanel());

	}

	public static void main(String args[])
	{
		UIManager.put("ScrollBarUI", GraphGuiScrollBarUI.class.getName());

		GraphGui test = new GraphGui("Pegel Andelfingen waterheight");
		test.pack();
		RefineryUtilities.centerFrameOnScreen(test);
		test.setVisible(true);
	}
}
