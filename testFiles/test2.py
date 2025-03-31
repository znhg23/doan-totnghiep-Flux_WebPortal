import flux
from flux.resource import list
# write into text.txt "Hello world"

h = flux.Flux('local:///tmp/flux-AmiyL6/local-0')
r = list.ResourceListRPC(h)
print(r.get())