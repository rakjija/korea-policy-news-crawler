FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
RUN npm run build

RUN mkdir -p /app/dist/views
COPY ./src/views /app/dist/views

EXPOSE 3000

CMD ["npm", "start"]
