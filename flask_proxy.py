from flask import Flask, request

app = Flask(__name__)


# Catch-all route for undefined routes
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
def catch_all(path):
    # Print headers for undefined routes
    print(f"Request Headers for undefined route '/{path}':")
    for header, value in request.headers.items():
        print(f"{header}: {value}")
    
    return f"The route '/{path}' does not exist. Check your console for headers!", 404

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4200, debug=True)