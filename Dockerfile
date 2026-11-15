FROM node:20-alpine

WORKDIR /app

COPY package.json server.js index.html proposal.html starlink_proposal.html README.md ./
COPY *.pdf ./

ENV NODE_ENV=production
ENV PORT=10000

EXPOSE 10000

CMD ["node", "server.js"]
