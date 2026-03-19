import os

def list_files(directory):
    if not os.path.exists(directory):
        print(f"Directory '{directory}' does not exist.")
        return

    print(f"Listing files in '{directory}':")
    try:
        files = os.listdir(directory)
        for f in files[:20]:  # List first 20 files
            print(f)
    except Exception as e:
        print(f"Error listing files: {e}")

if __name__ == "__main__":
    list_files("AMMC_Resultats")
