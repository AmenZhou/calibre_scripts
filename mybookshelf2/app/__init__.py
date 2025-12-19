from flask import Flask, render_template, request
from flask_login import login_required
from flask_sqlalchemy import SQLAlchemy
import decimal

app = Flask(__name__)
app.config.from_object('settings')
db=SQLAlchemy(app)


#fix for decimals
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError
app.config['RESTFUL_JSON']={'default': decimal_default }


# Add global response headers for better browser compatibility
@app.after_request
def add_security_headers(response):
    """Add security and compatibility headers for all responses"""
    # Ensure proper content type
    if response.content_type and 'charset' not in response.content_type:
        if response.content_type.startswith('text/html'):
            response.content_type = 'text/html; charset=utf-8'
    
    # Add CORS headers if needed (for API requests)
    origin = request.headers.get('Origin')
    if origin:
        from app.cors import check_cors
        if check_cors(origin):
            response.headers.add('Access-Control-Allow-Origin', origin)
            response.headers.add('Access-Control-Allow-Credentials', 'true')
            response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,PATCH,OPTIONS')
            response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
            response.headers.add('Vary', 'Origin')
    
    # Prevent Chrome from trying to upgrade to HTTPS
    response.headers.add('Strict-Transport-Security', 'max-age=0')
    
    return response


#blueprints
def register_blueprints():
    from app.api import bp as api_bp
    from app.access import bp as access_bp
    from app.minimal import bp as minimal_bp
    
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(access_bp)
    app.register_blueprint(minimal_bp)
    
register_blueprints()