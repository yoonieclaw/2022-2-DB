from flask import Blueprint, render_template, request, redirect, url_for, session
import db


bp = Blueprint('preferences', __name__, url_prefix='/preferences', template_folder='templates')

# ===================================================
# 
# ===================================================

@bp.route('/', methods=['GET', 'POST'])
def preferences():
    if request.method == 'GET' and 'database' in session:
        return render_template('preferences.html')

    else:
        return redirect(url_for('dblogin.dblogin'))


# ===================================================
# 대표 속성 추가
# ===================================================
@bp.route('/addnew', methods=['POST'])
def addattr():
    new_name = request.form.get('newname')
    new_type = request.form.get('type')

    conn = db.get_db()
    with conn:
        cur = conn.cursor()

        table_name, name_column = ('STD_REPR_ATTR', 'repr_attr_name') if new_type == 'ra' else ('STD_JOIN_KEY', 'key_name')
        new_attr_stmt = f'INSERT INTO `{table_name}`(`{name_column}`) VALUES ("{new_name}")'
        cur.execute(new_attr_stmt)

        conn.commit()

    return redirect(url_for('preferences.preferences'))


# ===================================================
# 대표 속성 수정
# ===================================================
@bp.route('/edit', methods=['POST'])
def edit():
    prev_name = list(request.form.keys())[0]
    new_name = request.form[prev_name]
    target_type = request.form.get('type')

    conn = db.get_db()
    with conn:
        cur = conn.cursor()

        table_name, name_column = ('STD_REPR_ATTR', 'repr_attr_name') if target_type == 'ra' else ('STD_JOIN_KEY', 'key_name')

        change_stmt = f'UPDATE `{table_name}` SET `{name_column}`="{new_name}" WHERE `{name_column}`="{prev_name}"'
        cur.execute(change_stmt)

        conn.commit()

    return redirect(url_for('preferences.preferences'))


# ===================================================
# 대표 속성 삭제
# ===================================================
@bp.route('/delete', methods=['POST'])
def delete():
    target_name = request.form.get('target_name')
    target_type = request.form.get('type')

    conn = db.get_db()
    with conn:
        cur = conn.cursor()

        table_name, id_column, name_column = ('STD_REPR_ATTR', 'repr_attr_id', 'repr_attr_name')\
            if target_type == 'ra' else ('STD_JOIN_KEY', 'key_id', 'key_name')

        get_id_stmt = f'SELECT `{id_column}` FROM `{table_name}` WHERE `{name_column}`="{target_name}"'
        cur.execute(get_id_stmt)
        target_id, = cur.fetchone()

        delete_stmt = f'DELETE FROM `{table_name}` WHERE `{id_column}`={target_id}'
        cur.execute(delete_stmt)

        conn.commit()

    return redirect(url_for('preferences.preferences'))
