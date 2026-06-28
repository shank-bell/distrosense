from ml_service.gateway.grpc_gateway import serve

if __name__ == "__main__":
    print("[MLService] Starting gRPC ML inference gateway...")
    serve()