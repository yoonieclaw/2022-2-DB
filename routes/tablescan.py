from flask import Blueprint, render_template, session, redirect, url_for, request
import db


bp = Blueprint('tablescan', __name__, url_prefix='/tablescan',
               template_folder='templates')


# ===================================================
# 대상 테이블 선택
# ===================================================
@bp.route('/', methods=['GET'])
def tablescan():
    if 'database' in session:
        return render_template('tablescan.html')
    else:
        return redirect(url_for('dblogin.dblogin'))


# ===================================================
# 테이블 속성 도메인 스캔
# ===================================================
@bp.route('/<table_name>', methods=['GET', 'POST'])
def scan_table(table_name):
    if request.method == 'GET':
        tabledname = table_name

        conn = db.get_db()
        with conn:
            cur = conn.cursor()

            # 총 레코드 수
            cur.execute(
                'SELECT COUNT(*) FROM attr WHERE table_name = %s', [tabledname])
            total_record = cur.fetchall()[0][0]

            # 속성
            attr = []
            datype = []
            null = []
            record = []
            distinct = []
            candidate = []
            isnumeric = []

            cur.execute(
                'SELECT attr_name, data_type, null_count, record_count, distinct_count, is_candidate, is_numeric FROM attr WHERE table_name = %s', [tabledname])
            for attr_name, data_type, null_count, record_count, distinct_count, is_candidate, is_numeric in cur.fetchall():
                attr.append(attr_name)
                datype.append(data_type)
                null.append(null_count)
                record.append(record_count)
                distinct.append(distinct_count)
                candidate.append(is_candidate)
                isnumeric.append(is_numeric)

            # 수치속성
            zero = []
            minv = []
            maxv = []

            cur.execute(
                'SELECT zero_count, min_value, max_value FROM numeric_attr WHERE table_name = %s', [tabledname])
            for zero_count, min_value, max_value in cur.fetchall():
                zero.append(zero_count)
                minv.append(min_value)
                maxv.append(max_value)

            # 범주속성
            symbol = []

            cur.execute(
                'SELECT symbol_count FROM categorical_attr WHERE table_name = %s', [tabledname])
            for symbol_count in cur.fetchall():
                symbol.append(symbol_count[0])

            # 대표속성
            attrname_RA = []
            reprattr = []

            cur.execute(
                'SELECT attr_name, repr_attr_name FROM repr_attr, std_repr_attr WHERE repr_attr.repr_attr_id = std_repr_attr.repr_attr_id AND table_name = %s', [tabledname])
            for attr_name, repr_attr_name in cur.fetchall():
                attrname_RA.append(attr_name)
                reprattr.append(repr_attr_name)

            # 대표결합키
            attrname_JK = []
            joinkey = []

            cur.execute(
                'SELECT attr_name, key_name FROM join_key, std_join_key WHERE join_key.join_key_id = std_join_key.key_id AND table_name = %s', [tabledname])
            for attr_name, key_name in cur.fetchall():
                attrname_JK.append(attr_name)
                joinkey.append(key_name)

            # boxplot
            numeric_attr = ""
            cur.execute(
                'SELECT attr_name FROM attr WHERE is_numeric = "T" AND table_name = %s', [tabledname])
            for attr_name in cur.fetchall():
                numeric_attr += str(attr_name[0]) + ','

            numeric_attr = numeric_attr.rstrip(',')

            cur.execute(f'SELECT {numeric_attr} FROM {tabledname}')
            numeric_attr_data = cur.fetchall()

        return render_template(
            'tablescanresult.html',
            table_name=tabledname,
            total_record=total_record,
            attr=attr,
            datype=datype,
            null=null,
            record=record,
            distinct=distinct,
            candidate=candidate,
            isnumeric=isnumeric,
            zero=zero,
            minv=minv,
            maxv=maxv,
            symbol=symbol,
            attr_name_RA=attrname_RA,
            attr_name_JK=attrname_JK,
            repr_attr=reprattr,
            join_key=joinkey,
            numeric_attr=numeric_attr,
            numeric_attr_data=numeric_attr_data
        )

    else:
        conn = db.get_db()

        with conn:
            cur = conn.cursor()

            repr_attr_list = {}
            cur.execute('SELECT `repr_attr_id`, `repr_attr_name` FROM `STD_REPR_ATTR`')
            for repr_attr_id, repr_attr_name in cur.fetchall():
                repr_attr_list[repr_attr_name] = repr_attr_id

            join_key_list = {}
            cur.execute('SELECT `key_id`, `key_name` FROM `STD_JOIN_KEY`')
            for key_id, key_name in cur.fetchall():
                join_key_list[key_name] = key_id

            for key in request.form.keys():
                if request.form[key] == 'Null':
                    continue

                attr_name = key[:-3]

                if key[-2:] == 'RA':
                    repr_attr_name = request.form[key]
                    repr_attr_id = repr_attr_list[repr_attr_name]

                    stmt = f'INSERT INTO `REPR_ATTR` VALUES ("{table_name}", "{attr_name}", {repr_attr_id})'
                    print(stmt)
                    cur.execute(stmt)

                elif key[-2:] == 'JK':
                    join_key_name = request.form[key]
                    join_key_id = join_key_list[join_key_name]
                    
                    stmt = f'INSERT INTO `JOIN_KEY` VALUES ("{table_name}", "{attr_name}", {join_key_id})'
                    print(stmt)
                    cur.execute(stmt)

            cur.execute(f'UPDATE `TABLE_LIST` SET `is_scanned` = "T" WHERE `table_name` = "{table_name}"')

            conn.commit()

        return redirect(url_for('tablescan.scan_table', table_name = table_name))
