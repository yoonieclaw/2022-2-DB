from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify
import pymysql
import db


bp = Blueprint('result', __name__, url_prefix='/result', template_folder='templates')


"""
계속해서 쓰이는 변수들 설명
    result_root : 결과조회 페이지들의 URL 맨 앞에 항상 붙는 부분
    type : 제공할 서비스 종류. 
           html파일에서 if문을 통해 어떤 페이지를 보여줄지 구분하기 위한 용도 
"""

#처음 기본화면
@bp.route('/', methods=['GET'])
def select():
    global result_root 
    result_root = url_for('result.select')
    if 'database' in session:
        return render_template("result.html",result_root = result_root)
    else:
        return redirect(url_for('dblogin.dblogin'))




#스캔 결과조회 화면
@bp.route("/scan/")
def scan_select():
    global result_root 
    
    conn = db.get_db()
    cursor = conn.cursor()
    
    #스캔된 테이블들만 고르는 SQL문
    select_table_sql = """
        SELECT table_name
        FROM TABLE_LIST
        WHERE is_scanned = 'T'
    """
    
    cursor.execute(select_table_sql)
    semi_tables = cursor.fetchall()
    tables = [list(semi_tables[x]) for x in range(len(semi_tables))]
    
    #스캔된 테이블들을 담은 배열에, 각 테이블의 레코드 개수 추가
    for i in range(len(tables)) :
        count_record_sql = """
            SELECT count(*)
            FROM {0}
        """.format(tables[i][0])
        cursor.execute(count_record_sql)
        tables[i].append(cursor.fetchone()[0])
        
    
    return render_template("result.html",result_root = result_root,
                           type = 'scan_select', 
                           table_rows = tables)

#어떤 테이블을 선택했을 때, 그 테이블의 스캔결과를 보여주는 화면
@bp.route("/scan/<table_name>/")
def scan_result(table_name):
    global result_root
    
    conn = db.get_db()
    cursor = conn.cursor()
    
    #수치속성들을 불러오는 SQL문
    numerical_row_sql = """
        SELECT ATTR.attr_name, data_type, record_count, distinct_count, null_count, null_count/record_count,  zero_count, zero_count/record_count,  min_value, max_value, is_candidate
        From ATTR, NUMERIC_ATTR
        WHERE ATTR.table_name = NUMERIC_ATTR.table_name
        AND ATTR.attr_name = NUMERIC_ATTR.attr_name
        AND ATTR.table_name = '{0}'
    """.format(table_name)
    
    #범주속성들을 불러오는 SQL문
    categorical_row_sql = """
        SELECT ATTR.attr_name, data_type, record_count, distinct_count, null_count,  null_count/record_count,  symbol_count, symbol_count/record_count, is_candidate
        From ATTR, CATEGORICAL_ATTR
        WHERE ATTR.table_name = CATEGORICAL_ATTR.table_name 
        AND ATTR.attr_name = CATEGORICAL_ATTR.attr_name
        AND ATTR.table_name = '{0}'
    """.format(table_name)
    
    
    cursor.execute(numerical_row_sql)
    numeric_rows = cursor.fetchall()
    numerical_rows = [list(numeric_rows[x]) for x in range(len(numeric_rows))]
    
    cursor.execute(categorical_row_sql)
    categoric_rows = cursor.fetchall()
    categorical_rows = [list(categoric_rows[x]) for x in range(len(categoric_rows))]
    for i in range(len(numerical_rows)) :
        represent_attr_sql = """
            SELECT repr_attr_name
            FROM REPR_ATTR, STD_REPR_ATTR
            WHERE REPR_ATTR.repr_attr_id = STD_REPR_ATTR.repr_attr_id
            AND table_name = '{0}'
            AND attr_name = '{1}'
        """.format(table_name, numerical_rows[i][0])
        
        join_key_sql = """
            SELECT key_name
            FROM JOIN_KEY, STD_JOIN_KEY
            WHERE JOIN_KEY.join_key_id = STD_JOIN_KEY.key_id
            AND table_name = '{0}'
            AND attr_name = '{1}'
        """.format(table_name, numerical_rows[i][0])
        
        #이 아랫부분 수정 필요할수도 있음 - sql 반환값이 없을 경우 어떻게 되는지 모름
        cursor.execute(represent_attr_sql)
        semi_row = cursor.fetchone()
        if semi_row is None :
            val = "x"
        else :
            val = semi_row[0]
        numerical_rows[i].append(val)
        
        cursor.execute(join_key_sql)
        semi_row = cursor.fetchone()
        if semi_row is None :
            val = "x"
        else :
            val = semi_row[0]
        numerical_rows[i].append(val)
    
    for i in range(len(categorical_rows)) :
        represent_attr_sql = """
            SELECT repr_attr_name
            FROM REPR_ATTR, STD_REPR_ATTR
            WHERE REPR_ATTR.repr_attr_id = STD_REPR_ATTR.repr_attr_id
            AND table_name = '{0}'
            AND attr_name = '{1}'
        """.format(table_name, categorical_rows[i][0])
        
        join_key_sql = """
            SELECT key_name
            FROM JOIN_KEY, STD_JOIN_KEY
            WHERE JOIN_KEY.join_key_id = STD_JOIN_KEY.key_id
            AND table_name = '{0}'
            AND attr_name = '{1}'
        """.format(table_name, categorical_rows[i][0])
        
        #이 아랫부분 수정 필요할수도 있음 - sql 반환값이 없을 경우 어떻게 되는지 모름
        cursor.execute(represent_attr_sql)
        semi_row = cursor.fetchone()
        if semi_row is None :
            val = "x"
        else :
            val = semi_row[0]
        categorical_rows[i].append(val)
        
        cursor.execute(join_key_sql)
        semi_row = cursor.fetchone()
        if semi_row is None :
            val = "x"
        else :
            val = semi_row[0]
        categorical_rows[i].append(val)
        
    return render_template("result.html", result_root = result_root,
                           type = 'scan_result',
                           table_name = table_name,
                           numeric_rows = numerical_rows,
                           categoric_rows = categorical_rows)

#결합 결과들의 리스트를 보여주는 SQL문
#사용자가 선택한 조건들을 주소의 일부분으로 포함
@bp.route("/JOINlist/<type>/<used_table>/<join_ratio_limit>/<min_record_num_limit>/")
def single_select(type,used_table, join_ratio_limit, min_record_num_limit):
    global result_root

    conn = db.get_db()
    cursor = conn.cursor()

    #기본 sql문
    joined_result_sql = """
    SELECT * 
    FROM {0}_JOIN_TABLE_LIST
    WHERE id IS NOT NULL
    """.format(type)
    
    #사용자로부터 조건을 입력받지 않으면, NONE으로 조건에 대응되는 주소부분 값 설정
    #조건을 입력받을경우, 조건값에 대응되는 주소부분 값으로 설정(html 상에서)
    #주소를통해 조건이 있음을 인식 >> 그에 해당하는 SQL 조건문 추가
    if used_table != "NONE" :
        joined_result_sql += " AND (source_table_name = '{0}' OR target_table_name = '{0}')".format(used_table)
    
    if join_ratio_limit != "NONE" :
        joined_result_sql += " AND (source_success_rate >= '{0}' OR target_success_rate >= '{0}')".format(join_ratio_limit)
    
    if min_record_num_limit != "NONE" :
        joined_result_sql += " AND joined_record_count >= '{0}'".format(min_record_num_limit)
    
    cursor.execute(joined_result_sql)
    joined_result = cursor.fetchall()

    if type == "MULTIPLE":
        cursor.execute(f'SELECT * FROM multiple_join_table_list where is_complete = 0')
        join_request = cursor.fetchall()
        if join_request:
            join_request = join_request[0]
            cursor.execute(
                f'CREATE TABLE {join_request[8]} AS SELECT A.* FROM {join_request[2]} AS A INNER JOIN {join_request[5]} AS B ON A.{join_request[4]} = B.{join_request[7]}')
            cursor.execute(f'SELECT COUNT(*) FROM {join_request[8]}')
            joined_rec_num = cursor.fetchall()[0][0]
            source_success = round(joined_rec_num / join_request[3], 2)
            target_success = round(joined_rec_num / join_request[6], 2)
            finished = 1
            result_val = join_request[14]
            join_stat = "완료 " + result_val[3:8]
            cursor.execute(
                f'UPDATE MULTIPLE_JOIN_TABLE_LIST SET joined_record_count = {joined_rec_num}, source_success_rate = {source_success}, target_success_rate = {target_success}, is_complete = {finished}, join_status = "{join_stat}" WHERE id ={join_request[0]} AND inner_id ={join_request[1]}')
            conn.commit()
            return render_template("result.html",
                            type=type,
                            result_root=result_root,
                            joined_result=joined_result,
                            table_state=0
                            )
        else:
            return render_template("result.html",
                            type=type,
                            result_root=result_root,
                            joined_result=joined_result,
                            table_state=1
                            )
    
    return render_template("result.html",
                           type = type,
                           result_root = result_root,
                           joined_result = joined_result,
                           )

@bp.route("/JOINresult/<type>/<id>/<inner_id>/")
def join_result(type, id, inner_id) :
    
    global result_root
    
    conn = db.get_db()
    cursor = conn.cursor()


    if type == "SINGLE" :
        src_table = "단일결합테이블_{0}".format(id)

    else:
        src_table = "다중결합테이블_{0}_{1}".format(id, inner_id)
    
    table_result_sql = """
    SELECT *
    FROM {0}
    """.format(src_table)
    
    cursor.execute(table_result_sql)
    table_result = cursor.fetchall()
    
    return render_template("result.html",
                           type = 'result',
                           result_root = result_root,
                           table_result = table_result,
                           id = id,
                           inner_id = inner_id)