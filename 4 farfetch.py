from flask import Flask, request, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
import uuid
from werkzeug.security import generate_password_hash, check_password_hash
import datetime
from flask_marshmallow import Marshmallow
from functools import wraps
import jwt
from sqlalchemy import and_, func

app = Flask(__name__)

app.config.from_pyfile('config.py')
db = SQLAlchemy(app)
ma = Marshmallow(app)

DataOta = db.Table('rawDataOta', db.metadata, autoload=True, autoload_with=db.engine)


class otaSchema(ma.Schema):
    class Meta:
        fields = ("shopName", "publishTime", "content", 'uuid')


otas_schema = otaSchema(many=True)


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'access' in request.headers:
            token = jwt.encode(
                    {"content": '1'},
                    'secrete', algorithm="HS256")


        if not token:
            return jsonify({'message': 'Token is missing!'}), 401
        #
        # try:
        #     # data = jwt.decode(token, app.config['SECRET_KEY'])
        #     data = jwt.decode(token, 'secrete', algorithms=["HS256"])
        #
        #     current_user = db.session.query(DataOta).all()
        #
        # except:
        #     return jsonify({'message': 'Token is invalid!'}), 401

        return f(*args, **kwargs)

    return decorated


# @app.route('/data', methods=['GET'])
# @token_required
# def get_data():
#     all_data = db.session.query(DataOta).paginate(per_page=10, page=1)
#     total_page = all_data.pages
#     result = otas_schema.dump(all_data.items)
#
#     real_result = {}
#     page = {}
#     page["total page"] = total_page
#     real_result['data'] = result
#     real_result['paginate'] = page
#
#     print(real_result)
#     return jsonify(real_result)


@app.route('/real', methods=['POST'])
@token_required
def get_time():

    data = request.get_json()

    start_date = datetime.datetime.strptime(data['start_date'], '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime(data['end_date'], '%Y-%m-%d %H:%M:%S')

    all_data = db.session.query(DataOta).filter(
        db.and_(DataOta.c.publishTime <= end_date,
             DataOta.c.publishTime >= start_date)).order_by(DataOta.c.publishTime.desc()).paginate(per_page=data['per_page'],
                                                            page=data['page'])


    real_result = {}
    page = {}
    page["total page"] = all_data.pages
    real_result['data'] = otas_schema.dump(all_data.items)
    real_result['paginate'] = page

    return jsonify(real_result)


if __name__ == '__main__':
    app.run(debug=True)

