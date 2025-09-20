import matplotlib
matplotlib.use("Agg")  # ✅ prevents Tkinter errors, safe for Flask/web
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import io, base64
import pandas as pd
import numpy as np

def generate_summary_chart(summary_results, group_by="Region_Descr", selected_area=None, chart_type="bar", metric="Total_Records"):
    """
    Generate a summary chart (base64 PNG) from summarised data.
    """
    if not summary_results:
        return None

    try:
        df = pd.DataFrame(summary_results)

        if df.empty:
            return None

        # Build chart based on type and metric
        plt.figure(figsize=(10, 6))
        
        # Get the data for the selected metric
        x_labels = df['group_name'].fillna('Unknown')
        y_values = df[metric].fillna(0)
        
        # Set chart title based on metric
        metric_titles = {
            "Total_Records": "Total Records",
            "Total_Gas_Charges": "Gas Charges (Rs.)",
            "Total_Net_Bill": "Net Bill Amount (Rs.)",
            "Total_SCM_Consumed": "SCM Consumed"
        }
        
        title = f"{metric_titles.get(metric, metric.replace('_', ' ').title())} by {group_by.replace('_Descr','')}"
        
        if chart_type == "pie":
            plt.pie(y_values, labels=x_labels, autopct='%1.1f%%', startangle=90)
            plt.title(title)
            plt.axis('equal')
            
        elif chart_type == "line":
            plt.plot(range(len(x_labels)), y_values, marker='o', linewidth=2, markersize=8)
            plt.title(title)
            plt.xlabel(group_by.replace('_Descr', ''))
            plt.ylabel(metric_titles.get(metric, metric.replace('_', ' ').title()))
            plt.xticks(range(len(x_labels)), x_labels, rotation=45, ha='right')
            plt.grid(True, alpha=0.3)
            
        else:
            # Bar chart (default)
            bars = plt.bar(range(len(x_labels)), y_values, color=plt.cm.Set3(np.arange(len(x_labels))))
            plt.title(title)
            plt.xlabel(group_by.replace('_Descr', ''))
            plt.ylabel(metric_titles.get(metric, metric.replace('_', ' ').title()))
            plt.xticks(range(len(x_labels)), x_labels, rotation=45, ha='right')
            
            # Add value labels on bars
            for i, (bar, value) in enumerate(zip(bars, y_values)):
                if "Charges" in metric or "Bill" in metric:
                    # Format currency values
                    formatted_value = f"Rs. {int(value):,}"
                else:
                    # Format numeric values
                    formatted_value = f"{int(value):,}"
                
                plt.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.05,
                        formatted_value, ha='center', va='bottom', fontsize=9)

        # Format y-axis for currency values
        if "Charges" in metric or "Bill" in metric:
            plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, p: f"Rs. {int(x):,}"))
        else:
            plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, p: format(int(x), ',')))

        # Save to base64
        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format="png", dpi=100, bbox_inches='tight')
        buf.seek(0)
        chart_data = base64.b64encode(buf.getvalue()).decode("utf-8")
        plt.close()

        return chart_data
        
    except Exception as e:
        print(f"Error generating chart: {e}")
        return None