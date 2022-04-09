import yaml
from flask import Flask, request, json
from ScistarterAPI import ScistarterAPI

with open("scistarter_cfg.yml", "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

app = Flask(__name__)
api = ScistarterAPI()
api.load_opportunities_df(cfg['flask']['dataframe_path'])

@app.route('/update', methods=['POST'])
def update():
    is_lazy = bool(request.form.get('lazy', default=True, type=bool))
    prev_size = len(api.opportunities_df)
    api.load_opportunities_df(cfg['flask']['dataframe_path'])
    data = {'new_rows': len(api.opportunities_df) - prev_size}
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route('/recommendations', methods=['GET'])
def recommend():
    recommendations = []
    N = 10  # number of recommendations
    max_dist = None
    user_ip = request.form.get('ip')
    if request.form.get('max_dist') is not None:
        max_dist = request.form.get('max_dist')
    if request.form.get('N') is not None:
        N = request.form.get('N')
    if user_ip is not None:
        recommendations = list(api.recommend_user(user_ip, N, max_dist))
    return_dict = {'user_ip': user_ip,
                   'recommendations': recommendations,
                   'max_recommendations': N,
                   'max_distance': max_dist}
    response = app.response_class(
        response=json.dumps(return_dict),
        status=200,
        mimetype='application/json'
    )
    return response
