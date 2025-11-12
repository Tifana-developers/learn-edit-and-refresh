# Node18
FROM node:18-slim

WORKDIR /usr/src/app
COPY package.json package-lock.json* ./

RUN npm ci --only=production

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["npm", "start"]