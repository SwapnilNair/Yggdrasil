from flask import Flask                                                         
import _thread

data = 'Hello from the other side...'
app = Flask(__name__)

@app.route("/")
def main():
    return data

def flaskThread():
    app.run()

if __name__ == "__main__":
    _thread.start_new_thread(flaskThread, ())
    k = input()
    print(k+" It's working")
    while(True):
    	pass
    print("HELLO")