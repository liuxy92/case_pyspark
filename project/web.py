from bottle import route, run, static_file
import json

@route('/static/<filename>')
def server_static(filename):
  return static_file(filename, root="./static")

@route("/<name:re:.*\.html>")
def server_page(name):
  return static_file(name, root=".")

@route("/")
def index():
  return static_file("index.html", root=".")


run(host="0.0.0.0", port=9999)
