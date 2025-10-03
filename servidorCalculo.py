import time, argparse, os, sys, threading, datetime, concurrent.futures as cf
from typing import List, Tuple

import grpc

# Stubs generados
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'generated'))
import distcalc_pb2, distcalc_pb2_grpc         # Cliente - Coordinador
import calc_pb2, calc_pb2_grpc                 # Coordinador - Operadores

class InfoOperador:
    """DTO + stub para un operador remoto CalcService."""
    def __init__(self, nombre: str, host: str, puerto: int, timeout_rpc: float = 4.0):
        self.nombre = nombre
        self.host = host
        self.puerto = puerto
        self.addr = f"{host}:{puerto}"
        self.timeout_rpc = timeout_rpc
        self.channel = grpc.insecure_channel(self.addr)
        self.stub = calc_pb2_grpc.CalcServiceStub(self.channel)
        self.vivo = False
        self.ultimo_ok = 0.0
        self.lock = threading.Lock()

    def ping(self) -> bool:
        """Ping usando Add(0,0)."""
        try:
            _ = self.stub.Add(calc_pb2.BinaryOpRequest(a=0.0, b=0.0), timeout=1.0)
            return True
        except Exception:
            return False

    def sumar_mitad_por_elemento(self, a_chunk: List[int], b_chunk: List[int], timeout_total: float) -> List[int]:
        """Suma elemento a elemento con RPC Add (una RPC por par)."""
        resultado = []
        t0 = time.time()
        for x, y in zip(a_chunk, b_chunk):
            restante = max(0.1, timeout_total - (time.time() - t0))
            resp = self.stub.Add(calc_pb2.BinaryOpRequest(a=float(x), b=float(y)), timeout=restante)
            if not resp.ok:
                raise RuntimeError(resp.error or "Add falló")
            resultado.append(int(resp.result))
        return resultado

# Healtchecker

class Repetidor(threading.Thread):
    """Ejecuta una función periódicamente."""
    def __init__(self, intervalo: float, funcion, nombre: str = "HealthChecker", daemon: bool = True):
        super().__init__(daemon=daemon, name=nombre)
        self.intervalo = intervalo
        self.funcion = funcion
        self._stop = threading.Event()

    def detener(self):
        self._stop.set()

    def run(self):
        while not self._stop.is_set():
            try:
                self.funcion()
            except Exception as e:
                print(f"[{self.name}] Error: {e}")
            finally:
                self._stop.wait(self.intervalo)

# Coordinador gRCP

class CoordinatorImpl(distcalc_pb2_grpc.CoordinatorServiceServicer):
    """Servidor de cálculo: divide A,B en dos mitades y usa operadores por RPC Add."""
    def __init__(self, operadores: List[Tuple[str, int]]):
        self.ops: List[InfoOperador] = [
            InfoOperador(nombre=f"operador-{i+1}", host=h, puerto=p) for i, (h, p) in enumerate(operadores)
        ]
        # timers
        self.intervalo_salud_seg = 3.0
        self.timeout_mitad_seg = 6.0

        self.hilo_salud = Repetidor(self.intervalo_salud_seg, self.verificar_salud, nombre="HealthChecker")
        self.hilo_salud.start()

    def _ahora(self) -> str:
        return datetime.datetime.now().strftime("%H:%M:%S")

    def verificar_salud(self):
        """Ping a cada operador e impresión VIVO/DOWN."""
        for op in self.ops:
            try:
                ok = op.ping()
                with op.lock:
                    op.vivo = ok
                    if ok:
                        op.ultimo_ok = time.time()
                estado = "OK" if ok else "NO-RESPONDE"
                print(f"[{self._ahora()}][salud] {op.nombre} {estado}")
            except Exception:
                with op.lock:
                    op.vivo = False
                print(f"[{self._ahora()}][salud] {op.nombre} DOWN")

    def _resolver_mitad_con_failover(self, idx: int, a_chunk: List[int], b_chunk: List[int]) -> List[int]:
        """Intenta en el preferido (0→op1, 1→op2), reintenta en el otro si falla/timeout."""
        prefer_index = 0 if idx == 0 else (1 if len(self.ops) > 1 else 0)
        orden = [prefer_index] + [i for i in range(len(self.ops)) if i != prefer_index]
        ultimo_error = None
        for i in orden:
            op = self.ops[i]
            try:
                print(f"[coord] Enviando mitad {idx} a {op.nombre} ({op.addr}) -> A={a_chunk} B={b_chunk}")
                with cf.ThreadPoolExecutor(max_workers=1) as tpe:
                    fut = tpe.submit(op.sumar_mitad_por_elemento, a_chunk, b_chunk, self.timeout_mitad_seg)
                    return fut.result(timeout=self.timeout_mitad_seg)
            except Exception as e:
                ultimo_error = e
                print(f"[coord] {op.nombre} falló en mitad {idx}: {e}")
                continue
        raise RuntimeError(f"No fue posible resolver la mitad {idx}: {ultimo_error}")

    # RPC del coordinador
    def SumArrays(self, request, context):
        """Suma A[] + B[] dividiendo en dos mitades (RPC Add por elemento)."""
        a = list(request.a)
        b = list(request.b)

        if len(a) != len(b):
            return distcalc_pb2.SumArraysReply(ok=False, error="Longitudes distintas", result=[])

        n = len(a)
        mitad = n // 2
        partes = [(0, a[:mitad], b[:mitad]), (1, a[mitad:], b[mitad:])]

        # Estado actual de operadores 
        estados = []
        for op in self.ops:
            vivo = op.ping()
            estados.append(distcalc_pb2.OperatorInfo(nombre=op.nombre, host=op.host, puerto=op.puerto, vivo=vivo))

        print(f"[coord] Nueva tarea con n={n} -> mitades={len(partes)}")
        for idx, aa, bb in partes:
            print(f"[coord] Mitad {idx}: A={aa} B={bb}")

        t0 = time.time()
        try:
            res0 = self._resolver_mitad_con_failover(0, partes[0][1], partes[0][2])
            res1 = self._resolver_mitad_con_failover(1, partes[1][1], partes[1][2])
            resultado_final = res0 + res1
            elapsed = time.time() - t0
            print(f"[coord] Tarea RESUELTA → resultado final = {resultado_final} (elapsed={elapsed:.4f}s)")
            return distcalc_pb2.SumArraysReply(result=resultado_final, ok=True, error="", elapsed=elapsed, operadores=estados)
        except Exception as e:
            elapsed = time.time() - t0
            print(f"[coord] Error en tarea: {e}")
            return distcalc_pb2.SumArraysReply(result=[], ok=False, error=str(e), elapsed=elapsed, operadores=estados)

def servir(host: str, port: int, operadores: List[Tuple[str, int]]):
    server = grpc.server(cf.ThreadPoolExecutor(max_workers=32))
    distcalc_pb2_grpc.add_CoordinatorServiceServicer_to_server(CoordinatorImpl(operadores), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    print(f"[coord] Coordinador gRPC escuchando en {host}:{port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("[coord] Deteniendo...")
        server.stop(0)

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Servidor de Cálculo (Coordinador) gRPC")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=5000)
    ap.add_argument("--operadores", nargs="+", default=["127.0.0.1:6001", "127.0.0.1:6002"], help="host:port ...")
    args = ap.parse_args()
    ops: List[Tuple[str, int]] = []
    for spec in args.operadores:
        h, p = spec.split(":")
        ops.append((h, int(p)))
    servir(args.host, args.port, ops)
