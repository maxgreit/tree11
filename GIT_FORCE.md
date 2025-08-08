# Force pull voorbeeld (overschrijft lokaal):
# git fetch origin && git reset --hard origin/main

# Force push (lokaal leidend):
# 1) backup remote main
#    BACKUP=backup/main-20250808-145554; git fetch origin; git push origin origin/main:refs/heads/
# 2) force push
#    git push --force-with-lease origin main

