"""
Configurações SSL para contornar problemas de certificado
"""

import ssl
import warnings

import urllib3


def disable_ssl_warnings():
    """
    Desabilita verificação SSL e warnings relacionados.

    Esta função deve ser chamada no início da aplicação para evitar
    erros de SSL em ambientes de desenvolvimento ou com certificados
    auto-assinados.

    ⚠️ ATENÇÃO: Usar apenas em ambientes de desenvolvimento!
    Em produção, configure certificados SSL adequados.
    """

    # Desabilita verificação SSL globalmente
    ssl._create_default_https_context = ssl._create_unverified_context

    # Desabilita warnings do urllib3 sobre SSL
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Desabilita warnings gerais sobre SSL
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    print("⚠️  SSL verification disabled - use only in development!")


# Chama automaticamente quando o módulo é importado
disable_ssl_warnings()  # Chama automaticamente quando o módulo é importado
disable_ssl_warnings()  # Chama automaticamente quando o módulo é importado
disable_ssl_warnings()
