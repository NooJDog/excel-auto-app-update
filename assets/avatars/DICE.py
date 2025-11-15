import os, requests, random

# 所有可用風格（2025 最新版）
styles = [
    "adventurer", "adventurer-neutral", "avataaars", "avataaars-neutral",
    "big-ears", "big-ears-neutral", "big-smile", "bottts", "croodles",
    "croodles-neutral", "fun-emoji", "icons", "identicon", "initials",
    "lorelei", "lorelei-neutral", "micah", "miniavs", "notionists",
    "notionists-neutral", "open-peeps", "personas", "pixel-art",
    "pixel-art-neutral", "rings", "shapes", "thumbs"
]

output_dir = "avatars_all"
os.makedirs(output_dir, exist_ok=True)

for style in styles:
    for i in range(1, 11):
        seed = random.randint(1000, 9999)
        url = f"https://api.dicebear.com/7.x/{style}/png?seed={seed}&size=128"
        filename = f"{style}_{i:02d}.png"
        path = os.path.join(output_dir, filename)

        try:
            img = requests.get(url, timeout=10).content
            with open(path, "wb") as f:
                f.write(img)
            print(f"✅ {filename}")
        except Exception as e:
            print(f"❌ {filename}: {e}")
