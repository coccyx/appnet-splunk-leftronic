until python bin/leftronic.py; do
    echo "Server 'leftronic.py' crashed with exit code $?.  Respawning.." >&2
    sleep 1
done
