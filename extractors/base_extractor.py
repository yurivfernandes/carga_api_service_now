"""
Classe base para extração de dados do ServiceNow
"""

import datetime
import time
from time import sleep
from typing import Any, Dict, List, Optional

import polars as pl
import requests

# Importa configurações SSL (desabilita verificação SSL automaticamente)
import ssl_config  # noqa: F401
from config import Config, flatten_reference_fields


class BaseServiceNowExtractor:
    """Classe base para extração de dados do ServiceNow"""

    def __init__(self):
        self.base_url = Config.SERVICENOW_BASE_URL
        self.auth = Config.get_servicenow_auth()
        self.headers = Config.get_servicenow_headers()
        self.api_metrics = {
            "total_requests": 0,
            "total_api_time": 0.0,
            "failed_requests": 0,
        }

    def make_request(
        self, endpoint: str, params: Dict[str, Any]
    ) -> List[Dict]:
        """Faz uma requisição para a API do ServiceNow"""
        url = f"{self.base_url}/{endpoint}"

        start_time = time.time()
        self.api_metrics["total_requests"] += 1

        try:
            response = requests.get(
                url,
                auth=self.auth,
                headers=self.headers,
                params=params,
                verify=False,  # Desabilita verificação SSL
            )
            response.raise_for_status()

            request_time = time.time() - start_time
            self.api_metrics["total_api_time"] += request_time

            return response.json().get("result", [])
        except requests.exceptions.RequestException as e:
            request_time = time.time() - start_time
            self.api_metrics["total_api_time"] += request_time
            self.api_metrics["failed_requests"] += 1
            print(f"❌ Erro na requisição: {e}")
            return []

    def paginated_request(
        self, endpoint: str, base_params: Dict[str, Any], limit: int = 10000
    ) -> List[Dict]:
        """Faz requisições paginadas para buscar todos os dados"""
        all_data = []
        offset = 0

        while True:
            params = {
                **base_params,
                "sysparm_limit": limit,
                "sysparm_offset": offset,
            }

            result_page = self.make_request(endpoint, params)

            if not result_page:
                print("✅ Fim dos resultados.")
                break

            all_data.extend(result_page)
            print(f"📦 Página lida com sucesso: +{len(result_page)} registros")
            offset += limit

            # Evita overload na API
            sleep(0.2)

        return all_data

    def process_data(self, data: List[Dict]) -> List[Dict]:
        """Processa os dados, aplicando flatten nos campos de referência"""
        processed_data = []

        for item in data:
            if not item:
                continue

            processed_item = flatten_reference_fields(item)
            processed_data.append(processed_item)

        return processed_data

    def get_date_range(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> tuple:
        """Retorna o range de datas para filtro. Se não especificado, usa os últimos 2 dias"""
        if start_date and end_date:
            return start_date, end_date

        today = datetime.datetime.now().date()
        end_date = today.strftime("%Y-%m-%d")
        start_date = (today - datetime.timedelta(days=2)).strftime("%Y-%m-%d")

        return start_date, end_date

    def get_api_metrics(self) -> dict:
        """Retorna métricas de performance da API"""
        return self.api_metrics.copy()

    def print_api_metrics(self, extractor_name: str = ""):
        """Imprime métricas de performance da API"""
        metrics = self.api_metrics
        avg_time = metrics["total_api_time"] / max(
            metrics["total_requests"], 1
        )
        success_rate = (
            (metrics["total_requests"] - metrics["failed_requests"])
            / max(metrics["total_requests"], 1)
        ) * 100

        print(f"\n📊 Métricas API {extractor_name}:")
        print(f"   ├── Total de requisições: {metrics['total_requests']}")
        print(f"   ├── Requisições falharam: {metrics['failed_requests']}")
        print(f"   ├── Taxa de sucesso: {success_rate:.1f}%")
        print(f"   ├── Tempo total API: {metrics['total_api_time']:.2f}s")
        print(f"   └── Tempo médio por requisição: {avg_time:.3f}s")

    def extract_data(self, *args, **kwargs) -> pl.DataFrame:
        """Método abstrato para extrair dados - deve ser implementado pelas classes filhas"""
        raise NotImplementedError(
            "Subclasses devem implementar o método extract_data"
        )
