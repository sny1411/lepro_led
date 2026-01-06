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
import random
from aiomqtt import Client

# Configuration - MODIFIEZ ICI
LEPRO_EMAIL = "votre_email@example.com"
LEPRO_PASSWORD = "votre_mot_de_passe"

# Endpoints API Lepro (identiques au projet)
LOGIN_URL = "https://openapi.leaguelighting.com/app/user/login"
USER_PROFILE_URL = "https://openapi.leaguelighting.com/app/user/profile"
DEVICE_LIST_URL = "https://openapi.leaguelighting.com/app/device/list"

class LeproMQTTSniffer:
    def __init__(self, email, password):
        self.email = email
        self.password = password
        self.bearer_token = None
        self.client_id = self._generate_client_id()
        self.mqtt_info = None
        self.devices = []

    def _generate_client_id(self):
        """Génère un client ID persistant basé sur l'email"""
        mac_seed = hashlib.md5(self.email.encode()).hexdigest()[:12]
        mac = ":".join(mac_seed[i:i+2] for i in range(0, 12, 2))
        return f"T{mac}"

    async def login(self):
        """Authentification auprès de l'API Lepro"""
        print(f"🔐 Connexion avec {self.email}...")

        async with aiohttp.ClientSession() as session:
            payload = {
                "client": "android",
                "client_id": self.client_id,
                "email": self.email,
                "password": self.password
            }

            async with session.post(LOGIN_URL, json=payload) as response:
                if response.status != 200:
                    raise Exception(f"Erreur login: {response.status}")

                data = await response.json()
                self.bearer_token = data.get("data", {}).get("bearer")

                if not self.bearer_token:
                    raise Exception("Pas de bearer token reçu")

                print(f"✅ Connecté ! Token: {self.bearer_token[:20]}...")

    async def get_mqtt_info(self):
        """Récupère les informations MQTT (broker, certificats)"""
        print("📡 Récupération des infos MQTT...")

        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {self.bearer_token}"}

            async with session.get(USER_PROFILE_URL, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f"Erreur profile: {response.status}")

                data = await response.json()
                profile = data.get("data", {})
                self.mqtt_info = profile.get("mqtt_info", {})

                print(f"✅ Broker MQTT: {self.mqtt_info.get('host')}:{self.mqtt_info.get('port')}")

    async def get_devices(self):
        """Récupère la liste des appareils"""
        print("💡 Récupération des appareils...")

        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {self.bearer_token}"}

            async with session.post(DEVICE_LIST_URL, headers=headers, json={}) as response:
                if response.status != 200:
                    raise Exception(f"Erreur devices: {response.status}")

                data = await response.json()
                self.devices = data.get("data", [])

                for dev in self.devices:
                    print(f"  📱 {dev.get('name')} (ID: {dev.get('did')}) - Type: {dev.get('series')}")

    async def download_certificates(self):
        """Télécharge les certificats SSL"""
        print("🔒 Téléchargement des certificats SSL...")

        os.makedirs("certs", exist_ok=True)

        async with aiohttp.ClientSession() as session:
            # Certificat root CA
            async with session.get(self.mqtt_info["root"]) as resp:
                with open("certs/ca.pem", "wb") as f:
                    f.write(await resp.read())

            # Certificat client
            async with session.get(self.mqtt_info["cert"]) as resp:
                with open("certs/client.pem", "wb") as f:
                    f.write(await resp.read())

            # Copie de la clé client (depuis le projet)
            key_source = "custom_components/lepro_led/client_key.pem"
            if os.path.exists(key_source):
                import shutil
                shutil.copy(key_source, "certs/client_key.pem")
            else:
                print("⚠️  Clé client non trouvée, tentative sans...")

        print("✅ Certificats téléchargés dans ./certs/")

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

        try:
            ssl_context.load_cert_chain("certs/client.pem", "certs/client_key.pem")
        except:
            print("⚠️  Impossible de charger le certificat client")

        # Connexion au broker MQTT
        async with Client(
            hostname=self.mqtt_info["host"],
            port=self.mqtt_info["port"],
            client_id=self.client_id,
            tls_context=ssl_context
        ) as client:
            print(f"✅ Connecté au broker MQTT\n")

            # Souscription à tous les topics pour tous les appareils
            for device in self.devices:
                did = device["did"]
                topic = f"le/{did}/prp/#"
                await client.subscribe(topic)
                print(f"📻 Souscrit à: {topic}")

            # Souscription au topic client
            await client.subscribe(f"le/{self.client_id}/act/app/#")

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
                            other_keys = [k for k in d.keys() if k not in ['d1', 'd2', 'd50', 'd52', 'd60']]
                            if other_keys:
                                print(f"   ❓ Autres champs: {other_keys}")
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


async def main():
    print("="*60)
    print("   LEPRO MQTT SNIFFER - Capture de messages")
    print("="*60)
    print()

    sniffer = LeproMQTTSniffer(LEPRO_EMAIL, LEPRO_PASSWORD)

    try:
        await sniffer.login()
        await sniffer.get_mqtt_info()
        await sniffer.get_devices()
        await sniffer.download_certificates()
        await sniffer.start_mqtt_capture()

    except KeyboardInterrupt:
        print("\n\n⏹️  Capture arrêtée par l'utilisateur")
    except Exception as e:
        print(f"\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
