# curl -X POST -s http://127.0.0.1:8000/v1/completions -H "Content-Type: application/json" -d '{"prompt": "the us is ?","max_tokens": 10,"temperature": 0, "top_k":1}' | awk -F'"' '{print $22}'

curl -X POST -s http://127.0.0.1:8000/v1/completions -H "Content-Type: application/json" -d '{"prompt": "1 2 3 4 5 ","max_tokens": 10,"temperature": 0, "top_k":1}' | awk -F'"' '{print $22}'