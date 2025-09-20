from flask import Flask, render_template, request, redirect, url_for, session, jsonify 
import mysql.connector
from dotenv import load_dotenv
import os 
from mysql.connector import Error
from flask_moment import Moment
import logging
from chart import generate_summary_chart


app = Flask(__name__)
moment = Moment(app)
app.secret_key = 'your_secret_key_here'  

# Configure logging to see what's happening
logging.basicConfig(level=logging.DEBUG)

# Configure MySQL connection
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'database': os.getenv('DB_NAME')
}

def get_db_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            return connection
    except Error as e:
        app.logger.error(f"Database connection error: {e}")
        return None

@app.route('/')
def index():
    return redirect(url_for("login"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if "username" in session:
        return redirect(url_for("gen_report"))
        
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        if username == "admin" and password == "123":
            session["username"] = username
            return redirect(url_for("gen_report"))
        else:
            return render_template("login.html", error="Invalid username or password")

    return render_template("login.html")

@app.route("/gen_report")
def gen_report():
    if "username" not in session:
        return redirect(url_for("login"))
    return render_template("gen_report.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/clear-session")
def clear_session():
    session.clear()
    return redirect(url_for("login"))

# FIXED: Add error handling and logging to billing months endpoint
@app.route('/get_billing_months')
def get_billing_months():
    if "username" not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conn = get_db_connection()
    if not conn:
        app.logger.error("Failed to connect to database for billing months")
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        # FIXED: Test if the table exists first
        cursor.execute("SHOW TABLES LIKE 'ngrlng_sale_202506'")
        if not cursor.fetchone():
            app.logger.error("Table ngrlng_sale_202506 does not exist")
            return jsonify({'error': 'Table ngrlng_sale_202506 not found'}), 500
        
        # FIXED: More robust query with better error handling
        cursor.execute("""
            SELECT DISTINCT Billing_Month 
            FROM ngrlng_sale_202506
            WHERE Billing_Month IS NOT NULL AND Billing_Month != ''
            ORDER BY Billing_Month DESC
            LIMIT 100
        """)
        
        results = cursor.fetchall()
        app.logger.info(f"Found {len(results)} billing months")
        
        if not results:
            # FIXED: If no billing months found, return a helpful message
            return jsonify([{'Billing_Month': 'No billing months available'}])
        
        return jsonify(results)
        
    except Error as e:
        app.logger.error(f"Database error in get_billing_months: {e}")
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# FIXED: Add error handling and logging to customer classes endpoint
@app.route('/get_cust_classes')
def get_cust_classes():
    if "username" not in session:
        return jsonify({'error': 'Not authenticated'}), 401
    
    conn = get_db_connection()
    if not conn:
        app.logger.error("Failed to connect to database for customer classes")
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        # FIXED: Test if the table exists first
        cursor.execute("SHOW TABLES LIKE 'ngrlng_sale_202506'")
        if not cursor.fetchone():
            app.logger.error("Table ngrlng_sale_202506 does not exist")
            return jsonify({'error': 'Table ngrlng_sale_202506 not found'}), 500
        
        cursor.execute("""
            SELECT DISTINCT Cust_Cl_Cd 
            FROM ngrlng_sale_202506
            WHERE Cust_Cl_Cd IS NOT NULL AND Cust_Cl_Cd != ''
            ORDER BY Cust_Cl_Cd
            LIMIT 100
        """)
        
        results = cursor.fetchall()
        app.logger.info(f"Found {len(results)} customer classes")
        
        if not results:
            return jsonify([{'Cust_Cl_Cd': 'No customer classes available'}])
        
        return jsonify(results)
        
    except Error as e:
        app.logger.error(f"Database error in get_cust_classes: {e}")
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# FIXED: Improved units endpoint with better error handling
@app.route('/get_units')
def get_units():
    if "username" not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    billing_month = request.args.get('billing_month')
    cust_cl_cd = request.args.get('cust_cl_cd')

    if not billing_month:
        return jsonify([])

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT DISTINCT r.Unit_Cd, r.Unit_Descr 
            FROM ssgc_reports r
            INNER JOIN ngrlng_sale_202506 s ON r.Area_Cd = s.AREA
            WHERE s.Billing_Month = %s
        """
        params = [billing_month]

        if cust_cl_cd:
            query += " AND s.Cust_Cl_Cd = %s"
            params.append(cust_cl_cd)

        query += """
            AND r.Unit_Cd IS NOT NULL AND r.Unit_Descr IS NOT NULL
            AND r.Unit_Cd != '' AND r.Unit_Descr != ''
            ORDER BY r.Unit_Descr
            LIMIT 100
        """

        cursor.execute(query, tuple(params))
        units = cursor.fetchall()
        return jsonify(units)

    except Error as e:
        app.logger.error(f"Database error in get_units: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route('/get_regions')
def get_regions():
    if "username" not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    unit_cd = request.args.get('unit')
    billing_month = request.args.get('billing_month')
    cust_cl_cd = request.args.get('cust_cl_cd')

    if not unit_cd or not billing_month:
        return jsonify([])

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT DISTINCT r.Region_Cd, r.Region_Descr 
            FROM ssgc_reports r
            INNER JOIN ngrlng_sale_202506 s ON r.Area_Cd = s.AREA
            WHERE r.Unit_Cd = %s AND s.Billing_Month = %s
        """
        params = [unit_cd, billing_month]

        if cust_cl_cd:
            query += " AND s.Cust_Cl_Cd = %s"
            params.append(cust_cl_cd)

        query += """
            AND r.Region_Cd IS NOT NULL AND r.Region_Descr IS NOT NULL
            AND r.Region_Cd != '' AND r.Region_Descr != ''
            ORDER BY r.Region_Descr
            LIMIT 100
        """

        cursor.execute(query, tuple(params))
        regions = cursor.fetchall()
        return jsonify(regions)

    except Error as e:
        app.logger.error(f"Database error in get_regions: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route('/get_zones')
def get_zones():
    if "username" not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    region_cd = request.args.get('region')
    billing_month = request.args.get('billing_month')
    cust_cl_cd = request.args.get('cust_cl_cd')

    if not region_cd or not billing_month:
        return jsonify([])

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT DISTINCT r.Zone_Cd, r.Zone_Descr 
            FROM ssgc_reports r
            INNER JOIN ngrlng_sale_202506 s ON r.Area_Cd = s.AREA
            WHERE r.Region_Cd = %s AND s.Billing_Month = %s
        """
        params = [region_cd, billing_month]

        if cust_cl_cd:
            query += " AND s.Cust_Cl_Cd = %s"
            params.append(cust_cl_cd)

        query += """
            AND r.Zone_Cd IS NOT NULL AND r.Zone_Descr IS NOT NULL
            AND r.Zone_Cd != '' AND r.Zone_Descr != ''
            ORDER BY r.Zone_Descr
            LIMIT 100
        """

        cursor.execute(query, tuple(params))
        zones = cursor.fetchall()
        return jsonify(zones)

    except Error as e:
        app.logger.error(f"Database error in get_zones: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route('/get_subzones')
def get_subzones():
    if "username" not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    zone_cd = request.args.get('zone')
    billing_month = request.args.get('billing_month')
    cust_cl_cd = request.args.get('cust_cl_cd')

    if not zone_cd or not billing_month:
        return jsonify([])

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT DISTINCT r.SubZone_Cd, r.SubZone_Descr 
            FROM ssgc_reports r
            INNER JOIN ngrlng_sale_202506 s ON r.Area_Cd = s.AREA
            WHERE r.Zone_Cd = %s AND s.Billing_Month = %s
        """
        params = [zone_cd, billing_month]

        if cust_cl_cd:
            query += " AND s.Cust_Cl_Cd = %s"
            params.append(cust_cl_cd)

        query += """
            AND r.SubZone_Cd IS NOT NULL AND r.SubZone_Descr IS NOT NULL
            AND r.SubZone_Cd != '' AND r.SubZone_Descr != ''
            ORDER BY r.SubZone_Descr
            LIMIT 100
        """

        cursor.execute(query, tuple(params))
        subzones = cursor.fetchall()
        return jsonify(subzones)

    except Error as e:
        app.logger.error(f"Database error in get_subzones: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route('/get_areas')
def get_areas():
    if "username" not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    subzone_cd = request.args.get('subzone')
    billing_month = request.args.get('billing_month')
    cust_cl_cd = request.args.get('cust_cl_cd')

    if not subzone_cd or not billing_month:
        return jsonify([])

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT DISTINCT r.Area_Cd, r.Area_Descr 
            FROM ssgc_reports r
            INNER JOIN ngrlng_sale_202506 s ON r.Area_Cd = s.AREA
            WHERE r.SubZone_Cd = %s AND s.Billing_Month = %s
        """
        params = [subzone_cd, billing_month]

        if cust_cl_cd:
            query += " AND s.Cust_Cl_Cd = %s"
            params.append(cust_cl_cd)

        query += """
            AND r.Area_Cd IS NOT NULL AND r.Area_Descr IS NOT NULL
            AND r.Area_Cd != '' AND r.Area_Descr != ''
            ORDER BY r.Area_Descr
            LIMIT 100
        """

        cursor.execute(query, tuple(params))
        areas = cursor.fetchall()
        return jsonify(areas)

    except Error as e:
        app.logger.error(f"Database error in get_areas: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# FIXED: Improved generate_report with better validation
@app.route('/generate_report', methods=['POST'])
def generate_report():
    if "username" not in session:
        return jsonify({'error': 'Not authenticated'}), 401

    # FIXED: Add validation for required fields
    billing_month = request.form.get('billing_month')
    cust_cl_cd = request.form.get('cust_cl_cd')
    
    if not billing_month:
        # FIXED: Redirect back with error message if required fields missing
        return redirect(url_for('gen_report', error='Billing Month is required'))

    # Get all form data
    unit = request.form.get('unit') or None
    region = request.form.get('region') or None
    zone = request.form.get('zone') or None
    subzone = request.form.get('subzone') or None
    area = request.form.get('area') or None

    session['report_filters'] = {
        'billing_month': billing_month,
        'cust_cl_cd': cust_cl_cd,
        'unit': unit,
        'region': region,
        'zone': zone,
        'subzone': subzone,
        'area': area,
    }

    # Preserve page size if it exists in request
    per_page = request.form.get('per_page') or request.args.get('per_page') or 50
    return redirect(url_for('report_results', page=1, per_page=per_page))

# FIXED: Improved report_results with better error handling
@app.route("/report_results")
def report_results():
    if "username" not in session:
        return redirect(url_for("login"))
    
    filters = session.get('report_filters', {})
    if not filters:
        return redirect(url_for("gen_report"))

    # Get pagination parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    
    # Validate per_page to prevent excessive values
    if per_page not in [10, 25, 50, 100, 250, 500]:
        per_page = 50

    # Calculate offset
    offset = (page - 1) * per_page

    # FIXED: Add validation for required filters
  # FIX: only billing month is required
    if not filters.get('billing_month'):
        return redirect(url_for('gen_report'))


    # Base query components
    base_where = """
  WHERE s.Billing_Month = %s
  AND (%s IS NULL OR s.Cust_Cl_Cd = %s)
  AND (%s IS NULL OR r.Unit_Cd = %s)
  AND (%s IS NULL OR r.Region_Cd = %s)
  AND (%s IS NULL OR r.Zone_Cd = %s)
  AND (%s IS NULL OR r.SubZone_Cd = %s)
  AND (%s IS NULL OR r.Area_Cd = %s)
"""

    params = [
        filters.get('billing_month'),
        filters.get('cust_cl_cd'), filters.get('cust_cl_cd'),
        filters.get('unit'), filters.get('unit'),
        filters.get('region'), filters.get('region'),
        filters.get('zone'), filters.get('zone'),
        filters.get('subzone'), filters.get('subzone'),
        filters.get('area'), filters.get('area')
    ]

    # Add optional filters
   

    # Count total records
    count_query = f"""
    SELECT COUNT(*) as total
    FROM ssgc_reports r
    INNER JOIN ngrlng_sale_202506 s ON r.Area_Cd = s.AREA
    {base_where}
    """

    # Main query with pagination
    main_query = f"""
    SELECT 
        r.*,
        s.Billing_Month,
        s.Cust_Cl_Cd,
        s.Gas_Charges,
        s.Meter_Rent,
        s.GST,
        s.Total_SCM,
        s.Total_MMBTU,
        s.Less_Prov_Bills,
        s.Arrears,
        s.Total_Cur_Bill,
        s.Total_Net_Bill,
        s.LPS_Charged,
        s.LPS,
        s.Debit_Dr,
        s.Credit_Cr,
        s.Last_Payment,
        s.Misc_Adj,
        s.Others_Adj,
        s.Other_Amt,
        s.Other_CM,
        s.Other_MMBTU,
        s.Op_Bal,
        s.Cl_Bal,
        s.WoglamtCM,
        s.WoglamtMMBTU,
        s.NP_Fixed_Charges,
        s.P_Fixed_Charges,
        s.Levy_Charges,
        s.Levy_Adj,
        s.NP_Fix_Adj,
        s.P_Fix_Adj
    FROM ssgc_reports r
    INNER JOIN ngrlng_sale_202506 s ON r.Area_Cd = s.AREA
    {base_where}
    LIMIT %s OFFSET %s
    """

    # Add pagination params
    main_params = params + [per_page, offset]

    conn = get_db_connection()
    total_records = 0
    results = []
    
    if conn:
        try:
            cursor = conn.cursor(dictionary=True)
            
            # Get total count
            cursor.execute(count_query, tuple(params))
            count_result = cursor.fetchone()
            total_records = count_result['total'] if count_result else 0
            
            # Get paginated results
            cursor.execute(main_query, tuple(main_params))
            results = cursor.fetchall()
            
            app.logger.info(f"Found {total_records} total records, showing {len(results)} on page {page}")
            
        except Error as e:
            app.logger.error(f"Database error in report_results: {e}")
            # FIXED: Handle database errors gracefully
            total_records = 0
            results = []
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # Calculate total pages
    total_pages = (total_records + per_page - 1) // per_page if total_records > 0 else 1

    return render_template("report_results.html", 
                         results=results, 
                         filters=filters,
                         page=page,
                         per_page=per_page,
                         total_records=total_records,
                         total_pages=total_pages)

@app.route('/generate_summary_report', methods=['POST'])
def generate_summary_report():
    if "username" not in session:
        return redirect(url_for("login"))
    
    # Get all filter parameters from the form
    filters = {
        'billing_month': request.form.get('billing_month'),
        'cust_cl_cd': request.form.get('cust_cl_cd'),
        'unit': request.form.get('unit'),
        'region': request.form.get('region'),
        'zone': request.form.get('zone'),
        'subzone': request.form.get('subzone'),
        'area': request.form.get('area'),
    }
    
    # Store filters in session
    session['summary_filters'] = filters
    
    return redirect(url_for('summary_report'))

@app.route("/summary_report")
def summary_report():
    if "username" not in session:
        return redirect(url_for("login"))
    
    # ✅ Reuse filters from detailed report
    filters = session.get('report_filters', {})
    if not filters:
        return redirect(url_for("gen_report"))

    # Get grouping level
    group_by = request.args.get('group_by', 'Region_Descr')

    # --- Base WHERE with proper filtering ---
    base_where = """
    WHERE s.Billing_Month = %s
    AND (%s IS NULL OR s.Cust_Cl_Cd = %s)
"""
    params = [filters.get('billing_month'),
                filters.get('cust_cl_cd'), filters.get('cust_cl_cd')]


    # Add geographic filters if they exist
    geographic_filters = [
        ('unit', 'r.Unit_Cd'),
        ('region', 'r.Region_Cd'),
        ('zone', 'r.Zone_Cd'),
        ('subzone', 'r.SubZone_Cd'),
        ('area', 'r.Area_Cd')
    ]
    
    for filter_name, column_name in geographic_filters:
        filter_value = filters.get(filter_name)
        if filter_value:
            base_where += f" AND {column_name} = %s"
            params.append(filter_value)

    # ✅ Pick group-by columns dynamically and build SELECT accordingly
    if group_by == "Unit_Descr":
        group_by_cols = ["r.Unit_Descr"]
        select_columns = ["r.Unit_Descr", "NULL as Region_Descr", "NULL as Zone_Descr", "NULL as SubZone_Descr", "NULL as Area_Descr"]
    elif group_by == "Region_Descr":
        group_by_cols = ["r.Unit_Descr", "r.Region_Descr"]
        select_columns = ["r.Unit_Descr", "r.Region_Descr", "NULL as Zone_Descr", "NULL as SubZone_Descr", "NULL as Area_Descr"]
    elif group_by == "Zone_Descr":
        group_by_cols = ["r.Unit_Descr", "r.Region_Descr", "r.Zone_Descr"]
        select_columns = ["r.Unit_Descr", "r.Region_Descr", "r.Zone_Descr", "NULL as SubZone_Descr", "NULL as Area_Descr"]
    elif group_by == "SubZone_Descr":
        group_by_cols = ["r.Unit_Descr", "r.Region_Descr", "r.Zone_Descr", "r.SubZone_Descr"]
        select_columns = ["r.Unit_Descr", "r.Region_Descr", "r.Zone_Descr", "r.SubZone_Descr", "NULL as Area_Descr"]
    else:  # Area_Descr
        group_by_cols = ["r.Unit_Descr", "r.Region_Descr", "r.Zone_Descr", "r.SubZone_Descr", "r.Area_Descr"]
        select_columns = ["r.Unit_Descr", "r.Region_Descr", "r.Zone_Descr", "r.SubZone_Descr", "r.Area_Descr"]

    group_by_clause = "GROUP BY " + ", ".join(group_by_cols)
    order_by_clause = "ORDER BY " + ", ".join(group_by_cols)

    metrics_columns = [
        "COUNT(*) as Total_Records",
        "COALESCE(SUM(s.Gas_Charges), 0) as Total_Gas_Charges",
        "COALESCE(AVG(s.Gas_Charges), 0) as Avg_Gas_Charges",
        "COALESCE(SUM(s.Total_Net_Bill), 0) as Total_Net_Bill",
        "COALESCE(AVG(s.Total_Net_Bill), 0) as Avg_Net_Bill",
        "COALESCE(SUM(s.Meter_Rent), 0) as Total_Meter_Rent",
        "COALESCE(SUM(s.GST), 0) as Total_GST",
        "COALESCE(SUM(s.Arrears), 0) as Total_Arrears",
        "COALESCE(SUM(s.Last_Payment), 0) as Total_Last_Payments",
        "COALESCE(SUM(s.Total_SCM), 0) as Total_SCM_Consumed",
        "COALESCE(AVG(s.Total_SCM), 0) as Avg_SCM_Per_Account",
        "COALESCE(SUM(s.Total_MMBTU), 0) as Total_MMBTU_Consumed",
        "COALESCE(AVG(s.Total_MMBTU), 0) as Avg_MMBTU_Per_Account"
    ]

    summary_query = f"""
    SELECT {', '.join(select_columns + metrics_columns)}
    FROM ssgc_reports r
    INNER JOIN ngrlng_sale_202506 s ON r.Area_Cd = s.AREA
    {base_where}
    {group_by_clause}
    {order_by_clause}
    """

    summary_results = []
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor(dictionary=True)
            app.logger.info(f"Summary SQL: {summary_query}")
            app.logger.info(f"Summary SQL params: {params}")
            cursor.execute(summary_query, tuple(params))
            summary_results = cursor.fetchall()
            
            # Log the first few results for debugging
            for i, row in enumerate(summary_results[:3]):
                app.logger.info(f"Row {i}: {row}")
                
        except Error as e:
            app.logger.error(f"Database error in summary_report: {e}")
            summary_results = []
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    return render_template(
        "summary_report.html",
        summary_results=summary_results,
        filters=filters,
        group_by=group_by
    )


# --------- Custom Jinja Filters ---------
@app.template_filter('format_number')
def format_number(value):
    try:
        return "{:,.0f}".format(float(value))
    except (ValueError, TypeError):
        return value

@app.template_filter('format_currency')
def format_currency(value):
    try:
        return "Rs. {:,.2f}".format(float(value))
    except (ValueError, TypeError):
        return value

@app.template_filter('format_decimal')
def format_decimal(value):
    try:
        return "{:,.2f}".format(float(value))
    except (ValueError, TypeError):
        return value

@app.route("/chart_view")
def chart_view():
    if "username" not in session:
        return redirect(url_for("login"))
    
    # Get chart parameters from request
    group_by = request.args.get('group_by', 'Region_Descr')
    selected_area = request.args.get('area')
    chart_type = request.args.get('chart_type', 'bar')
    
    # Get filters from session
    filters = session.get('report_filters', {})
    if not filters:
        return redirect(url_for("gen_report"))

    # --- Query for chart data ---
    base_where = """
    WHERE s.Billing_Month = %s
    AND (%s IS NULL OR s.Cust_Cl_Cd = %s)
    AND (%s IS NULL OR r.Unit_Cd = %s)
    AND (%s IS NULL OR r.Region_Cd = %s)
    AND (%s IS NULL OR r.Zone_Cd = %s)
    AND (%s IS NULL OR r.SubZone_Cd = %s)
    AND (%s IS NULL OR r.Area_Cd = %s)
    """

    params = [
        filters.get('billing_month'),
        filters.get('cust_cl_cd'), filters.get('cust_cl_cd'),
        filters.get('unit'), filters.get('unit'),
        filters.get('region'), filters.get('region'),
        filters.get('zone'), filters.get('zone'),
        filters.get('subzone'), filters.get('subzone'),
        filters.get('area'), filters.get('area')
    ]

   

    # Query for chart data
    chart_query = f"""
    SELECT 
        COALESCE(r.{group_by}, 'All') as group_name,
        COUNT(*) as Total_Records,
        COALESCE(SUM(s.Gas_Charges), 0) as Total_Gas_Charges,
        COALESCE(SUM(s.Total_Net_Bill), 0) as Total_Net_Bill,
        COALESCE(SUM(s.Meter_Rent), 0) as Total_Meter_Rent,
        COALESCE(SUM(s.GST), 0) as Total_GST,
        COALESCE(SUM(s.Arrears), 0) as Total_Arrears,
        COALESCE(SUM(s.Last_Payment), 0) as Total_Last_Payments,
        COALESCE(SUM(s.Total_SCM), 0) as Total_SCM_Consumed,
        COALESCE(SUM(s.Total_MMBTU), 0) as Total_MMBTU_Consumed
    FROM ssgc_reports r
    LEFT JOIN ngrlng_sale_202506 s ON r.Area_Cd = s.AREA
    {base_where}
    GROUP BY r.{group_by}
    ORDER BY group_name
    """

    chart_data = None
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(chart_query, tuple(params))
            chart_results = cursor.fetchall()
            
            if chart_results:
                # Generate charts for all metrics
                chart_data = {
                    "Total_Records": generate_summary_chart(
                        chart_results, 
                        group_by=group_by, 
                        selected_area=selected_area, 
                        chart_type=chart_type,
                        metric="Total_Records"
                    ),
                    "Total_Gas_Charges": generate_summary_chart(
                        chart_results, 
                        group_by=group_by, 
                        selected_area=selected_area, 
                        chart_type=chart_type,
                        metric="Total_Gas_Charges"
                    ),
                    "Total_Net_Bill": generate_summary_chart(
                        chart_results, 
                        group_by=group_by, 
                        selected_area=selected_area, 
                        chart_type=chart_type,
                        metric="Total_Net_Bill"
                    ),
                    "Total_SCM_Consumed": generate_summary_chart(
                        chart_results, 
                        group_by=group_by, 
                        selected_area=selected_area, 
                        chart_type=chart_type,
                        metric="Total_SCM_Consumed"
                    )
                }
                
        except Error as e:
            app.logger.error(f"Database error in chart_view: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    return render_template(
        "chart_view.html",
        chart_data=chart_data,
        group_by=group_by,
        chart_type=chart_type,
        selected_area=selected_area,
        filters=filters
    )


   
if __name__ == '__main__':
    app.run(debug=True)