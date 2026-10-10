"""Real worker network denial and approved CONNECT path, with count-only evidence."""

from p5_integrity import worker


def network() -> dict[str, dict[str, object]]:
    code = (
        "import json,socket,subprocess; "
        "from urllib.request import urlopen; "
        "direct=subprocess.run(['curl','--noproxy','*','--connect-timeout','3',"
        "'--max-time','5','--silent','https://example.com'],capture_output=True); "
        "assert direct.returncode!=0; "
        "\nfor target in ('example.com:443','localhost:443','127.0.0.1:443',"
        "'minio:9000','api.github.com:80','api.github.com.evil.invalid:443'):\n"
        " s=socket.create_connection(('model-egress',8080),timeout=5)\n"
        " s.sendall(('CONNECT '+target+' HTTP/1.1\\r\\n\\r\\n').encode())\n"
        " assert s.recv(1024).startswith(b'HTTP/1.1 403'); s.close()\n"
        "approved=subprocess.run(['curl','--proxy','http://model-egress:8080',"
        "'--connect-timeout','5','--max-time','20','--silent','--output','/dev/null',"
        "'--write-out','%{http_code}','https://api.github.com'],capture_output=True); "
        "assert approved.returncode==0 and approved.stdout==b'200'; "
        "print(json.dumps({'direct_external_denied':True,'unapproved_connect_denied':True,"
        "'denied_connect_cases':6,'approved_https_status':200,'proxy':json.load(urlopen("
        "'http://model-egress:8080/health',timeout=5))}))"
    )
    return {service: worker(service, code) for service in ("worker-1", "worker-2")}
