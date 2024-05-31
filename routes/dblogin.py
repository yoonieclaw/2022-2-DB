from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify
import pymysql
import pandas as pd
import db   


bp = Blueprint('dblogin', __name__, url_prefix='/dblogin',
               template_folder='templates')

# ===================================================
# DB 접속
# ===================================================
@bp.route('/', methods=['GET', 'POST'])
def dblogin():
    """
    1) DB 연결 X:
        - GET: dblogin.html 표시
        - POST: 입력된 정보로 DB 접속 시도, 성공 시 session에 DB 정보 저장
    """
    if 'database' not in session:
        if request.method == 'POST':
            DB = {}
            for key in request.form:
                DB[key] = request.form[key]
            DB['port'] = int(DB['port'])
            try:
                conn = pymysql.connect(**DB)
                conn.close()
                for key in DB:
                    session[key] = DB[key]
                return render_template(
                    'dblogin.html',
                    host=session['host'],
                    port=session['port'],
                    database=session['database'],
                    user=session['user'],
                    msg='DB 연결 성공')
            except pymysql.Error:
                return render_template("dblogin.html", msg='DB 연결 실패')
        else:
            return render_template("dblogin.html")
    else:
        return render_template(
            'dblogin.html',
            host=session['host'],
            port=session['port'],
            database=session['database'],
            user=session['user'])


# ===================================================
# CSV 파일 업로드
# ===================================================
@bp.route('/upload', methods=['GET', 'POST'])
def upload():
    """
    1) DB 연결 X: 
        - DB 연결 창으로 redirect ('/dblogin')
    2) DB 연결 O: 
        - GET: upload.html 표시
        - POST: 첨부된 csv 파일을 읽고 DB에 테이블 생성

    * DB 연결은 db.py의 get_db() 사용
    """
    if 'database' in session:
        if request.method == 'POST':
            file = request.files['csv']
            # 한글 입력시 오류 생겨서 encoding 처리했습니다.
            if file:
                df = pd.read_csv(file, encoding='utf-8')
            else:
                msg = '선택된 파일이 없습니다'
                return render_template('upload.html', msg=msg)

            conn = db.get_db()
            with conn:
                cur = conn.cursor()

                # CSV -> TABLE 생성
                table_name = file.filename.split(".")[0]
                all_table_list_stmt = 'SELECT `table_name` FROM `TABLE_LIST`'
                cur.execute(all_table_list_stmt)
                all_table_list = []
                for names, in cur.fetchall():
                    all_table_list.append(names)

                # 테이블 명 중복 체크
                if table_name not in all_table_list:
                    col_stmt = ''
                    for col in df.columns:
                        if df[col].dtype == int:
                            col_stmt += f'`{col}` INT, '
                        elif df[col].dtype == float:
                            col_stmt += f'`{col}` DOUBLE, '
                        else:
                            col_stmt += f'`{col}` TEXT COLLATE utf8_bin, ' # 테스트 데이터 참고하여 VARCHAR(100) -> TEXT (utf-8)로 변경하였습니다
                    create_stmt = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({col_stmt[:-2]})'
                    cur.execute(create_stmt)

                    for row in df.index:
                        stmt = ''
                        for col in df.columns:
                            data = df[col][row] if not pd.isnull(
                                df[col][row]) else 'NULL'
                            stmt += f"'{data}', " if df[col].dtype == object and data != 'NULL' else f"{data}, "
                        insert_stmt = f'INSERT INTO `{table_name}` VALUES ({stmt[:-2]})'
                        cur.execute(insert_stmt)

                    # TABLE_LIST에 추가
                    table_list_stmt = f'INSERT INTO `TABLE_LIST` VALUES ("{table_name}", "F")'
                    cur.execute(table_list_stmt)

                    # ATTR에 추가
                    count_stmt = f'SELECT COUNT(*) FROM {table_name}'
                    cur.execute(count_stmt)
                    record_count = cur.fetchone()[0]
                    attr_stmt = 'INSERT INTO `ATTR` VALUES '
                    catg_attr_stmt = 'INSERT INTO `CATEGORICAL_ATTR` VALUES '
                    catg_exists = False
                    numr_attr_stmt = 'INSERT INTO `NUMERIC_ATTR` VALUES '
                    numr_exists = False
                    for col in df.columns:
                        dtype, is_numeric = ('TEXT', 'F') if df[col].dtype == object \
                            else (('DOUBLE', 'T') if df[col].dtype == float else ('INT', 'T'))

                        cur.execute(
                            f'SELECT COUNT(*) FROM {table_name} WHERE `{col}` is NULL')
                        null_count = cur.fetchone()[0]

                        cur.execute(
                            f'SELECT COUNT(DISTINCT `{col}`) FROM {table_name}')
                        distinct_count = cur.fetchone()[0]

                        is_candidate = 'T' if distinct_count / record_count > 0.9 else 'F'

                        attr_stmt += f'("{table_name}", "{col}", "{dtype}", {null_count}, {record_count}, {distinct_count}, "{is_candidate}", "{is_numeric}"), '

                        # 범주속성 -> CATEGORICAL_ATTR
                        if dtype == 'TEXT':
                            cur.execute(f'SELECT `{col}` FROM {table_name}')
                            symbol_count = 0
                            for data, in cur.fetchall():
                                if data is not None and not data.isalnum():
                                    symbol_count += 1
                            catg_attr_stmt += f'("{table_name}", "{col}", {symbol_count}), '
                            catg_exists = True

                        # 수치속성 -> NUMERICAL_ATTR
                        else:
                            cur.execute(
                                f'SELECT COUNT(*) FROM {table_name} WHERE `{col}` = 0')
                            zero_count = cur.fetchone()[0]

                            cur.execute(
                                f'SELECT MAX(`{col}`) FROM {table_name}')
                            max_value = cur.fetchone()[0]

                            cur.execute(
                                f'SELECT MIN(`{col}`) FROM {table_name}')
                            min_value = cur.fetchone()[0]

                            numr_attr_stmt += f'("{table_name}", "{col}", {zero_count}, {min_value}, {max_value}), '
                            numr_exists = True

                    cur.execute(attr_stmt[:-2])
                    if catg_exists:
                        cur.execute(catg_attr_stmt[:-2])
                    if numr_exists:
                        cur.execute(numr_attr_stmt[:-2])

                    conn.commit()
                    msg = f'{table_name} 테이블 생성 성공'

                else:
                    msg = f'동일한 이름의 테이블이 존재합니다: {table_name}'

            return render_template('upload.html', msg=msg)
        else:
            return render_template('upload.html')
    else:
        return redirect(url_for('dblogin.dblogin'))


# ===================================================
# DB 연결 해제
# ===================================================
@bp.route('/disconnect', methods=['POST'])
def disconnect():
    """
    session에 저장된 DB 정보를 삭제
    """
    if 'database' in session:
        session.pop('host', None)
        session.pop('port', None)
        session.pop('database', None)
        session.pop('user', None)
        session.pop('password', None)

    return redirect(url_for('dblogin.dblogin'))


# ===================================================
# DB 접속정보
# ===================================================
@bp.route('/status', methods=['GET'])
def status():
    db_status = {
        "host": "-",
        "port": "-",
        "database": "-"
    }
    if 'database' in session:
        db_status['host'] = session['host']
        db_status['port'] = session['port']
        db_status['database'] = session['database']
        return jsonify(db_status)
    else:
        return jsonify(db_status)
