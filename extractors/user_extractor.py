"""
Extrator de usuários do ServiceNow com sincronização incremental
"""

import hashlib
from datetime import datetime, timedelta
from typing import List, Optional, Set

import polars as pl

from config import get_db_connection

from .base_extractor import BaseServiceNowExtractor


class UserExtractor(BaseServiceNowExtractor):
    """Extrator específico para usuários (sys_user) com sincronização inteligente"""

    def __init__(self):
        super().__init__()
        self.table_name = "sys_user"
        self.api_endpoint = "sys_user"

    def extract_data(self, force_full_sync: bool = False) -> pl.DataFrame:
        """
        Extrai dados de usuários de forma incremental

        Args:
            force_full_sync: Se True, força sincronização completa
        """
        print(f"👥 Iniciando extração de usuários (sys_user)")
        force_full_sync = True
        if force_full_sync:
            print("🔄 Sincronização COMPLETA forçada")
            return self._extract_all_users()
        else:
            print("⚡ Sincronização INCREMENTAL (apenas mudanças)")
            return self._extract_incremental_users()

    def _extract_all_users(self) -> pl.DataFrame:
        """Extrai todos os usuários (sincronização completa)"""
        print("📥 Buscando todos os usuários...")

        # Busca todos os usuários ativos primeiro
        params = {
            "sysparm_query": "active=true",
            "sysparm_fields": self._get_user_fields(),
        }

        active_users = self.paginated_request(self.api_endpoint, params)
        print(f"✅ {len(active_users)} usuários ativos encontrados")

        # Busca também usuários inativos que foram modificados recentemente (últimos 30 dias)
        cutoff_date = (datetime.now() - timedelta(days=30)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        params_inactive = {
            "sysparm_query": f"active=false^sys_updated_on>={cutoff_date}",
            "sysparm_fields": self._get_user_fields(),
        }

        inactive_users = self.paginated_request(
            self.api_endpoint, params_inactive
        )
        print(
            f"✅ {len(inactive_users)} usuários inativos modificados recentemente"
        )

        # Combina todos os usuários
        all_users = active_users + inactive_users

        # Remove duplicatas baseado no sys_id
        seen_ids = set()
        unique_users = []
        for user in all_users:
            if user.get("sys_id") not in seen_ids:
                unique_users.append(user)
                seen_ids.add(user.get("sys_id"))

        print(f"✅ Total de {len(unique_users)} usuários únicos extraídos")

        if unique_users:
            # Processa dados e adiciona hash para controle
            processed_users = self._process_and_hash_users(unique_users)
            return pl.DataFrame(processed_users)
        else:
            return pl.DataFrame()

    def _extract_incremental_users(self) -> pl.DataFrame:
        """Extrai apenas usuários modificados recentemente"""

        # 1. Determina data de última sincronização
        last_sync_date = self._get_last_sync_date()
        print(f"📅 Última sincronização: {last_sync_date}")

        # 2. Busca usuários modificados desde a última sincronização
        query = f"sys_updated_on>={last_sync_date}"

        params = {
            "sysparm_query": query,
            "sysparm_fields": self._get_user_fields(),
        }

        modified_users = self.paginated_request(self.api_endpoint, params)
        print(
            f"✅ {len(modified_users)} usuários modificados desde {last_sync_date}"
        )

        if modified_users:
            # Processa dados e adiciona hash
            processed_users = self._process_and_hash_users(modified_users)

            # 3. Identifica quais realmente mudaram (usando hash)
            users_to_update = self._filter_changed_users(processed_users)
            print(
                f"🔄 {len(users_to_update)} usuários com mudanças reais detectadas"
            )

            if users_to_update:
                return pl.DataFrame(users_to_update)

        print("ℹ️ Nenhuma mudança detectada nos usuários")
        return pl.DataFrame()

    def _get_user_fields(self) -> str:
        """Define campos a serem extraídos da API de usuários"""
        fields = [
            # Campos básicos
            "sys_id",
            "user_name",
            "name",
            "first_name",
            "last_name",
            "middle_name",
            # Contato
            "email",
            "phone",
            "mobile_phone",
            # Organizacional
            "company",
            "department",
            "location",
            "manager",
            "title",
            # Status
            "active",
            "locked_out",
            "web_service_access_only",
            # Login
            "last_login",
            "last_login_time",
            "failed_attempts",
            # Configurações
            "time_zone",
            "date_format",
            "time_format",
            # Auditoria
            "sys_created_on",
            "sys_created_by",
            "sys_updated_on",
            "sys_updated_by",
        ]

        return ",".join(fields)

    def _process_and_hash_users(self, users: List[dict]) -> List[dict]:
        """Processa usuários e adiciona hash para controle de mudanças"""
        processed_users = []

        for user in users:
            # Processa campos de referência
            processed_user = self.process_data([user])[0]

            # Adiciona timestamps ETL
            processed_user["etl_created_at"] = datetime.now()
            processed_user["etl_updated_at"] = datetime.now()

            # Calcula hash dos dados principais (exclui campos de auditoria ETL)
            hash_data = {
                k: v
                for k, v in processed_user.items()
                if not k.startswith("etl_")
                and k not in ["sys_created_on", "sys_updated_on"]
            }

            data_string = str(sorted(hash_data.items()))
            processed_user["etl_hash"] = hashlib.md5(
                data_string.encode()
            ).hexdigest()

            processed_users.append(processed_user)

        return processed_users

    def _filter_changed_users(self, users: List[dict]) -> List[dict]:
        """Filtra apenas usuários que realmente mudaram (baseado no hash)"""
        if not users:
            return []

        changed_users = []

        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                for user in users:
                    # Busca hash atual no banco
                    query = "SELECT etl_hash FROM sys_user WHERE sys_id = ?"
                    cursor.execute(query, user["sys_id"])
                    result = cursor.fetchone()

                    if result is None:
                        # Usuário novo
                        print(
                            f"➕ Novo usuário: {user.get('user_name', user.get('sys_id'))}"
                        )
                        changed_users.append(user)
                    elif result[0] != user["etl_hash"]:
                        # Usuário modificado
                        print(
                            f"🔄 Usuário modificado: {user.get('user_name', user.get('sys_id'))}"
                        )
                        changed_users.append(user)
                    # Caso contrário, usuário não mudou (hash igual)

        except Exception as e:
            print(f"⚠️ Erro ao filtrar usuários alterados: {e}")
            print("ℹ️ Retornando todos os usuários por segurança")
            return users

        return changed_users

    def _get_last_sync_date(self) -> str:
        """Obtém data da última sincronização de usuários"""
        default_date = (datetime.now() - timedelta(days=7)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                # Busca última atualização na tabela de usuários
                query = "SELECT MAX(etl_updated_at) FROM sys_user"
                cursor.execute(query)
                result = cursor.fetchone()

                if result and result[0]:
                    # Subtrai 1 hora para garantir sobreposição
                    last_update = result[0] - timedelta(hours=1)
                    return last_update.strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            print(f"⚠️ Erro ao obter última sincronização: {e}")

        print(f"ℹ️ Usando data padrão: {default_date}")
        return default_date

    def get_users_by_ids(self, user_ids: Set[str]) -> pl.DataFrame:
        """
        Busca usuários específicos por IDs (para resolver referências)

        Args:
            user_ids: Set de IDs de usuários para buscar
        """
        if not user_ids:
            return pl.DataFrame()

        print(f"🔍 Buscando {len(user_ids)} usuários específicos por ID...")

        # Divide em lotes para evitar URLs muito longas
        batch_size = 50
        all_users = []

        user_ids_list = list(user_ids)
        for i in range(0, len(user_ids_list), batch_size):
            batch_ids = user_ids_list[i : i + batch_size]
            ids_query = "^OR".join([f"sys_id={uid}" for uid in batch_ids])

            params = {
                "sysparm_query": ids_query,
                "sysparm_fields": self._get_user_fields(),
            }

            batch_users = self.paginated_request(self.api_endpoint, params)
            all_users.extend(batch_users)
            print(
                f"📥 Lote {i // batch_size + 1}: {len(batch_users)} usuários"
            )

        print(f"✅ Total de {len(all_users)} usuários específicos extraídos")

        if all_users:
            processed_users = self._process_and_hash_users(all_users)
            return pl.DataFrame(processed_users)
        else:
            return pl.DataFrame()

    def get_missing_user_ids_from_incidents(self) -> Set[str]:
        """Identifica IDs de usuários referenciados em incidentes mas não presentes na tabela sys_user"""
        missing_ids = set()

        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                # Busca IDs únicos de usuários referenciados em incidentes
                query = """
                SELECT DISTINCT user_id
                FROM (
                    SELECT resolved_by as user_id FROM incident WHERE resolved_by IS NOT NULL
                    UNION
                    SELECT opened_by as user_id FROM incident WHERE opened_by IS NOT NULL
                    UNION  
                    SELECT caller_id as user_id FROM incident WHERE caller_id IS NOT NULL
                ) as all_user_refs
                WHERE user_id NOT IN (SELECT sys_id FROM sys_user)
                """

                cursor.execute(query)
                results = cursor.fetchall()

                for row in results:
                    missing_ids.add(row[0])

                print(
                    f"🔍 {len(missing_ids)} usuários referenciados mas não encontrados na tabela"
                )

        except Exception as e:
            print(f"⚠️ Erro ao identificar usuários em falta: {e}")

        return missing_ids

    def sync_missing_users(self) -> pl.DataFrame:
        """Sincroniza usuários que estão referenciados em incidentes mas faltam na tabela"""
        print("🔄 Sincronizando usuários em falta...")

        missing_ids = self.get_missing_user_ids_from_incidents()

        if missing_ids:
            return self.get_users_by_ids(missing_ids)
        else:
            print("✅ Todos os usuários referenciados já estão sincronizados")
            return pl.DataFrame()


def main():
    """Função para testar o extractor"""
    extractor = UserExtractor()

    print("🧪 TESTANDO EXTRACTOR DE USUÁRIOS")
    print("=" * 50)

    # Teste incremental
    df_incremental = extractor.extract_data(force_full_sync=False)
    print(f"📊 Usuários incrementais: {len(df_incremental)} registros")

    # Teste de usuários em falta
    df_missing = extractor.sync_missing_users()
    print(f"📊 Usuários em falta: {len(df_missing)} registros")

    print("✅ Teste concluído!")


if __name__ == "__main__":
    main()
