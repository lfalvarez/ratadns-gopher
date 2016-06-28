from gopher import create_app

app = create_app("config.json")

if __name__ == "__main__":
    app.run(port=8080, debug=True, threaded=True, use_reloader=False)
