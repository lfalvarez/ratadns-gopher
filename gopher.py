from gopher.app import create_wsgi_app
if __name__ == "__main__":
    app = create_wsgi_app(__name__)
    app.run(port=8080, debug=True, threaded=True)
