"""
Script standalone pour capturer les messages MQTT de votre Lepro Ceiling Light
Usage: python mqtt_sniffer.py
"""

import asyncio
import aiohttp
import json
import ssl
import os
import hashlib
import time
import sys
from aiomqtt import Client

# Fix pour Windows : utiliser SelectorEventLoop au lieu de ProactorEventLoop
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Configuration - MODIFIEZ ICI
LEPRO_EMAIL = "votre_email@example.com"
LEPRO_PASSWORD = "votre_mot_de_passe"

# Endpoints API Lepro
LOGIN_URL = "https://api-eu-iot.lepro.com/user/login"
USER_PROFILE_URL = "https://api-eu-iot.lepro.com/user/profile"
FAMILY_LIST_URL = "https://api-eu-iot.lepro.com/family/list/timestamp/{timestamp}"
DEVICE_LIST_URL = "https://api-eu-iot.lepro.com/v3/device/list/fid/{fid}/timestamp/{timestamp}"

class LeproMQTTSniffer:
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.bearer_token = None
        self.mac = self._generate_mac()
        self.mqtt_info = None
        self.devices = []
        self.client_id = None

    def _generate_mac(self):
        """Génère une adresse MAC persistante basée sur l'email"""
        mac_seed = hashlib.md5(self.email.encode()).hexdigest()[:12]
        mac = ":".join(mac_seed[i:i+2] for i in range(0, 12, 2))
        return mac

    def _get_headers(self, with_auth=False, timestamp=None):
        """Génère les headers requis par l'API Lepro"""
        if timestamp is None:
            timestamp = str(int(time.time()))

        headers = {
            "Content-Type": "application/json",
            "App-Version": "1.0.9.202",
            "Device-Model": "custom_integration",
            "Device-System": "custom",
            "GMT": "+0",
            "Host": "api-eu-iot.lepro.com",
            "Language": "fr",
            "Platform": "2",
            "Screen-Size": "1536*2048",
            "Slanguage": "fr",
            "Timestamp": timestamp,
            "User-Agent": "LE/1.0.9.202 (Custom Integration)",
        }

        if with_auth and self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
            headers["Accept-Encoding"] = "gzip"

        return headers

    async def login(self):
        """Authentification auprès de l'API Lepro"""
        print(f"🔐 Connexion avec {self.email}...")

        timestamp = str(int(time.time()))
        payload = {
            "platform": "2",
            "account": self.email,
            "password": self.password,
            "mac": self.mac,
            "timestamp": timestamp,
            "language": "fr",
            "fcmToken": "",
        }

        async with aiohttp.ClientSession() as session:
            headers = self._get_headers(with_auth=False, timestamp=timestamp)

            async with session.post(LOGIN_URL, json=payload, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"Erreur login: {response.status}")

                data = await response.json()

                if data.get("code") != 0:
                    raise Exception(f"Login échoué: {data.get('msg')}")

                self.bearer_token = data.get("data", {}).get("token")

                if not self.bearer_token:
                    raise Exception("Pas de token reçu")

                print(f"✅ Connecté ! Token: {self.bearer_token[:20]}...")

    async def get_user_profile(self):
        """Récupère le profil utilisateur et les infos MQTT"""
        print("📡 Récupération du profil utilisateur...")

        async with aiohttp.ClientSession() as session:
            timestamp = str(int(time.time()))
            headers = self._get_headers(with_auth=True, timestamp=timestamp)

            async with session.get(USER_PROFILE_URL, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"Erreur profile: {response.status}")

                data = await response.json()
                profile = data.get("data", {})
                self.mqtt_info = profile.get("mqtt", {})
                uid = profile.get("uid")

                # Génère le client_id comme dans le projet original
                client_id_suffix = hashlib.sha256(self.email.encode()).hexdigest()[:32]
                self.client_id = f"lepro-app-{client_id_suffix}"

                print(f"✅ Profil récupéré (UID: {uid})")
                print(f"✅ Broker MQTT: {self.mqtt_info.get('host')}:{self.mqtt_info.get('port')}")

    async def get_family_and_devices(self):
        """Récupère la famille puis la liste des appareils"""
        print("👨‍👩‍👧‍👦 Récupération de la famille...")

        async with aiohttp.ClientSession() as session:
            timestamp = str(int(time.time()))
            headers = self._get_headers(with_auth=True, timestamp=timestamp)

            # Étape 1 : Récupérer la famille pour obtenir le fid
            family_url = FAMILY_LIST_URL.format(timestamp=timestamp)
            async with session.get(family_url, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"Erreur family list: {response.status}")

                data = await response.json()
                try:
                    fid = data["data"]["list"][0]["fid"]
                    print(f"✅ Famille trouvée (FID: {fid})")
                except (KeyError, IndexError) as e:
                    raise Exception(f"Impossible d'extraire le fid: {e}")

            # Étape 2 : Récupérer les appareils avec le fid
            print("💡 Récupération des appareils...")
            timestamp = str(int(time.time()))
            headers["Timestamp"] = timestamp
            device_url = DEVICE_LIST_URL.format(fid=fid, timestamp=timestamp)

            async with session.get(device_url, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"Erreur device list: {response.status}")

                data = await response.json()
                self.devices = data.get("data", {}).get("list", [])

                if not self.devices:
                    print("⚠️  Aucun appareil trouvé")
                else:
                    for dev in self.devices:
                        print(f"  📱 {dev.get('name')} (ID: {dev.get('did')}) - Série: {dev.get('series')}")

    async def download_certificates(self):
        """Télécharge les certificats SSL"""
        print("🔒 Téléchargement des certificats SSL...")

        os.makedirs("certs", exist_ok=True)

        async with aiohttp.ClientSession() as session:
            headers = self._get_headers(with_auth=True)

            # Certificat root CA
            async with session.get(self.mqtt_info["root"], headers=headers) as resp:
                with open("certs/ca.pem", "wb") as f:
                    f.write(await resp.read())
                print("  ✅ ca.pem téléchargé")

            # Certificat client
            async with session.get(self.mqtt_info["cert"], headers=headers) as resp:
                with open("certs/client.pem", "wb") as f:
                    f.write(await resp.read())
                print("  ✅ client.pem téléchargé")

            # Copie de la clé client depuis le projet
            key_source = "custom_components/lepro_led/client_key.pem"
            if os.path.exists(key_source):
                with open(key_source, "rb") as src:
                    with open("certs/client_key.pem", "wb") as dst:
                        dst.write(src.read())
                print("  ✅ client_key.pem copié")
            else:
                print("  ⚠️  client_key.pem non trouvé dans le projet, tentative sans...")

        print("✅ Certificats prêts")

    async def start_mqtt_capture(self):
        """Démarre la capture MQTT"""
        print("\n" + "="*60)
        print("🎯 DÉBUT DE LA CAPTURE MQTT")
        print("="*60)
        print("Contrôlez votre lampe depuis l'application mobile Lepro...")
        print("Les messages apparaîtront ci-dessous.\n")

        # Création du contexte SSL
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations("certs/ca.pem")

        if os.path.exists("certs/client_key.pem"):
            try:
                ssl_context.load_cert_chain("certs/client.pem", "certs/client_key.pem")
                print("✅ Certificats SSL chargés")
            except Exception as e:
                print(f"⚠️  Impossible de charger les certificats client: {e}")
        else:
            print("⚠️  Tentative de connexion sans certificat client...")

        # Connexion au broker MQTT
        try:
            async with Client(
                hostname=self.mqtt_info["host"],
                port=int(self.mqtt_info["port"]),
                identifier=self.client_id,
                tls_context=ssl_context,
                clean_session=True
            ) as client:
                print(f"✅ Connecté au broker MQTT avec client_id: {self.client_id}\n")

                # Souscription à tous les topics pour tous les appareils
                for device in self.devices:
                    did = device["did"]
                    topic = f"le/{did}/prp/#"
                    await client.subscribe(topic)
                    print(f"📻 Souscrit à: {topic}")

                print("\n" + "-"*60 + "\n")

                # Écoute des messages
                async for message in client.messages:
                    try:
                        topic = message.topic.value
                        payload_raw = message.payload.decode()

                        print(f"📨 TOPIC: {topic}")
                        print(f"📦 RAW: {payload_raw}")

                        # Tentative de parsing JSON
                        try:
                            payload = json.loads(payload_raw)
                            print(f"📋 JSON:")
                            print(json.dumps(payload, indent=2, ensure_ascii=False))

                            # Analyse spécifique des champs
                            if 'd' in payload:
                                d = payload['d']
                                print(f"\n🔍 ANALYSE DES DONNÉES:")
                                if 'd1' in d:
                                    print(f"   💡 Power (d1): {d['d1']} ({'ON' if d['d1'] else 'OFF'})")
                                if 'd2' in d:
                                    print(f"   🎨 Mode (d2): {d['d2']}")
                                if 'd50' in d:
                                    print(f"   🌈 Couleur/Effet (d50): {d['d50']}")
                                if 'd52' in d:
                                    print(f"   ☀️  Luminosité (d52): {d['d52']}")
                                if 'd60' in d:
                                    print(f"   ✨ Effets spéciaux (d60): {d['d60']}")

                                # Autres champs inconnus
                                known_keys = ['d1', 'd2', 'd50', 'd52', 'd60']
                                other_keys = [k for k in d.keys() if k not in known_keys]
                                if other_keys:
                                    print(f"   ❓ Autres champs découverts:")
                                    for k in other_keys:
                                        print(f"      {k}: {d[k]}")

                        except json.JSONDecodeError:
                            print(f"⚠️  Pas un JSON valide")

                        print("\n" + "-"*60 + "\n")

                        # Sauvegarde dans un fichier
                        with open("mqtt_capture.log", "a", encoding="utf-8") as f:
                            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}]\n")
                            f.write(f"Topic: {topic}\n")
                            f.write(f"Payload: {payload_raw}\n\n")

                    except Exception as e:
                        print(f"❌ Erreur traitement message: {e}")

        except Exception as e:
            print(f"\n❌ Erreur connexion MQTT: {e}")
            import traceback
            traceback.print_exc()


async def main():
    print("="*60)
    print("   LEPRO MQTT SNIFFER - Capture de messages")
    print("="*60)
    print()

    sniffer = LeproMQTTSniffer(LEPRO_EMAIL, LEPRO_PASSWORD)

    try:
        await sniffer.login()
        await sniffer.get_user_profile()
        await sniffer.get_family_and_devices()
        await sniffer.download_certificates()
        await sniffer.start_mqtt_capture()

    except KeyboardInterrupt:
        print("\n\n⏹️  Capture arrêtée par l'utilisateur (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
