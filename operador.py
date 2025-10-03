import time
from concurrent import futures
import grpc
import argparse, sys, os

# Hace visibles los módulos generados como top-level
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'generated'))
import calc_pb2, calc_pb2_grpc  # Stubs de los OPERADORES (calc.proto)

class CalcImpl(calc_pb2_grpc.CalcServiceServicer):
    """Servicio de operación: suma/resta/multiplica/divide escalares.
    En Diseño A imprimimos cada par (a,b) que llega a Add() para ver qué resuelve.
    """
    def __init__(self, delay_seg: float = 0.0, nombre: str = "operador"):
        self.delay_seg = delay_seg
        self.nombre = nombre

    def _delay(self):
        if self.delay_seg > 0:
            time.sleep(self.delay_seg)

    def Add(self, request, context):
        """Suma escalar a+b. Imprime el par recibido y el peer que llamó."""
        self._delay()
        try:
            peer = context.peer() if context is not None else "?"
            res = request.a + request.b
            print(f"[{self.nombre}] Add(a={request.a}, b={request.b}) <- {peer}  => {res}")
            return calc_pb2.OpResponse(result=res, ok=True)
        except Exception as e:
            return calc_pb2.OpResponse(result=0.0, ok=False, error=str(e))

    def Sub(self, request, context):
        self._delay()
        try:
            res = request.a - request.b
            return calc_pb2.OpResponse(result=res, ok=True)
        except Exception as e:
            return calc_pb2.OpResponse(result=0.0, ok=False, error=str(e))

    def Mul(self, request, context):
        self._delay()
        try:
            res = request.a * request.b
            return calc_pb2.OpResponse(result=res, ok=True)
        except Exception as e:
            return calc_pb2.OpResponse(result=0.0, ok=False, error=str(e))

    def Div(self, request, context):
        self._delay()
        try:
            if request.b == 0:
                return calc_pb2.OpResponse(result=0.0, ok=False, error="division by zero")
            res = request.a / request.b
            return calc_pb2.OpResponse(result=res, ok=True)
        except Exception as e:
            return calc_pb2.OpResponse(result=0.0, ok=False, error=str(e))

def servir(host: str, port: int, delay: float, name: str):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    calc_pb2_grpc.add_CalcServiceServicer_to_server(CalcImpl(delay, name), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    print(f"[{name}] Operador gRPC escuchando en {host}:{port} (delay={delay}s)")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print(f"[{name}] Deteniendo...")
        server.stop(0)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Servidor de Operación (Operador) gRPC")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=6001)
    ap.add_argument("--name", default="operador-1")
    ap.add_argument("--delay", type=float, default=0.0, help="retardo artificial por RPC (segundos)")
    args = ap.parse_args()
    servir(args.host, args.port, args.delay, args.name)
